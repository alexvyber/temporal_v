"""Disposable HTTP/2 peer for transport compression and retry fault injection.

It deliberately returns one fixed protobuf response; this is not an SDK transport.
No HPACK decoding or third-party Python modules are needed to inspect DATA frames.
"""
import gzip
import socket
import threading
import time
from otlp_receiver import frame, read_exact


class TransportReceiver:
    def __init__(self, failures=0, status=14, compressed_response=True):
        self.listener = socket.socket()
        self.listener.bind(('127.0.0.1', 0))
        self.listener.listen()
        self.listener.settimeout(.2)
        self.port = self.listener.getsockname()[1]
        self.stopping = threading.Event()
        self.failures = failures
        self.status = status
        self.compressed_response = compressed_response
        self.received = []
        self.errors = []
        self.connections = set()
        self.lock = threading.Lock()
        self.handlers = []
        self.thread = threading.Thread(target=self.accept, daemon=True)

    def handle(self, connection):
        try:
            connection.settimeout(10)
            assert read_exact(connection, 24) == b'PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n'
            connection.sendall(frame(4, 0, 0))
            streams = {}
            while not self.stopping.is_set():
                header = read_exact(connection, 9)
                size, kind, flags = int.from_bytes(header[:3], 'big'), header[3], header[4]
                stream = int.from_bytes(header[5:], 'big') & 0x7fffffff
                if size > 2 * 1024 * 1024: raise ValueError('oversized HTTP/2 frame')
                payload = read_exact(connection, size)
                if kind == 4 and not flags & 1: connection.sendall(frame(4, 1, 0))
                elif kind == 6 and not flags & 1: connection.sendall(frame(6, 1, 0, payload))
                elif kind == 0:
                    data = payload[1:len(payload) - payload[0]] if flags & 8 else payload
                    streams[stream] = streams.get(stream, b'') + data
                    if size:
                        increment = size.to_bytes(4, 'big')
                        connection.sendall(frame(8, 0, 0, increment) + frame(8, 0, stream, increment))
                    if flags & 1:
                        body = streams.pop(stream)
                        if len(body) < 5 or body[0] not in (0, 1) or int.from_bytes(body[1:5], 'big') != len(body) - 5:
                            raise ValueError('invalid gRPC framing')
                        decoded = gzip.decompress(body[5:]) if body[0] else body[5:]
                        with self.lock:
                            self.received.append({'compressed': bool(body[0]), 'body': decoded, 'time': time.monotonic()})
                            attempt = len(self.received)
                        # First request is the connection's GetSystemInfo handshake.
                        code = self.status if 1 < attempt <= 1 + self.failures else 0
                        headers = b'\x88\x0f\x10\x10application/grpc'
                        response = b'\x0a\x05probe'
                        if self.compressed_response:
                            headers += b'\x00\x0dgrpc-encoding\x04gzip'
                            response = gzip.compress(response, mtime=0)
                        framed = bytes([int(self.compressed_response)]) + len(response).to_bytes(4, 'big') + response
                        status = str(code).encode()
                        trailers = b'\x00\x0bgrpc-status' + bytes([len(status)]) + status
                        connection.sendall(frame(1, 4, stream, headers) + (frame(0, 0, stream, framed) if not code else b'') + frame(1, 5, stream, trailers))
                elif kind == 3: streams.pop(stream, None)
                elif kind == 7: return
        except (EOFError, OSError): pass
        except Exception as error: self.errors.append(repr(error))
        finally:
            with self.lock: self.connections.discard(connection)
            connection.close()

    def accept(self):
        while not self.stopping.is_set():
            try: connection, _ = self.listener.accept()
            except socket.timeout: continue
            except OSError: return
            with self.lock: self.connections.add(connection)
            handler = threading.Thread(target=self.handle, args=(connection,), daemon=True)
            self.handlers.append(handler)
            handler.start()

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stopping.set()
        self.listener.close()
        with self.lock:
            connections = list(self.connections)
        for connection in connections:
            try: connection.shutdown(socket.SHUT_RDWR)
            except OSError: pass
            connection.close()
        self.thread.join(timeout=2)
        for handler in self.handlers: handler.join(timeout=2)
