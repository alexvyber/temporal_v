"""Minimal loopback OTLP gRPC receiver, using HTTP/2 and unary gRPC wire framing.

A caller-supplied decoder can inspect metrics or trace export messages. No external Python
packages or network services are needed by the integration suite.
"""
import socket
import threading


def fields(data):
    offset = 0
    def varint():
        nonlocal offset
        value = 0
        for shift in range(0, 70, 7):
            if offset >= len(data): raise ValueError('truncated protobuf varint')
            byte = data[offset]; offset += 1
            value |= (byte & 127) << shift
            if byte < 128: return value
        raise ValueError('oversized protobuf varint')
    while offset < len(data):
        tag = varint(); number, wire = tag >> 3, tag & 7
        if not number: raise ValueError('invalid protobuf field number')
        if wire == 0: value = varint()
        elif wire in (1, 5):
            size = 8 if wire == 1 else 4
            value = data[offset:offset + size]; offset += size
        elif wire == 2:
            size = varint(); value = data[offset:offset + size]; offset += size
            if len(value) != size: raise ValueError('truncated protobuf bytes')
        else: raise ValueError('unexpected OTLP protobuf wire type')
        if offset > len(data): raise ValueError('truncated protobuf fixed field')
        yield number, wire, value


def metric_names(data):
    names = []
    for number, wire, resource in fields(data):
        if (number, wire) != (1, 2): continue
        for number, wire, scope in fields(resource):
            if (number, wire) != (2, 2): continue
            for number, wire, metric in fields(scope):
                if (number, wire) != (2, 2): continue
                names += [value.decode('utf-8') for number, wire, value in fields(metric) if (number, wire) == (1, 2)]
    return names


def frame(kind, flags, stream, payload=b''):
    return len(payload).to_bytes(3, 'big') + bytes([kind, flags]) + stream.to_bytes(4, 'big') + payload


def read_exact(connection, size):
    result = b''
    while len(result) < size:
        value = connection.recv(size - len(result))
        if not value: raise EOFError()
        result += value
    return result


class OtlpGrpcReceiver:
    def __init__(self, decoder=metric_names, tls_context=None):
        self.tls_context = tls_context
        self.decoder = decoder
        self.listener = socket.socket()
        self.listener.bind(('127.0.0.1', 0)); self.listener.listen(); self.listener.settimeout(.2)
        self.port = self.listener.getsockname()[1]
        self.stopping = threading.Event()
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
            connection.sendall(frame(4, 0, 0))  # Server SETTINGS.
            streams = {}
            while not self.stopping.is_set():
                header = read_exact(connection, 9)
                size, kind, flags, stream = int.from_bytes(header[:3], 'big'), header[3], header[4], int.from_bytes(header[5:], 'big') & 0x7fffffff
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
                        if len(body) < 5 or body[0] != 0 or int.from_bytes(body[1:5], 'big') != len(body) - 5:
                            raise ValueError('unexpected gRPC metrics framing/compression')
                        names = self.decoder(body[5:])
                        self.received.append((names, body[5:]))
                        # HPACK static :status=200; literal content-type using the
                        # static-table name at index 31. No dynamic table is used.
                        headers = b'\x88\x0f\x10\x10application/grpc'
                        trailers = b'\x00\x0bgrpc-status\x010'
                        connection.sendall(frame(1, 4, stream, headers) + frame(0, 0, stream, b'\x00\x00\x00\x00\x00') + frame(1, 5, stream, trailers))
                elif kind == 3: streams.pop(stream, None)
                elif kind == 7: return
        except (EOFError, OSError): pass
        except Exception as error: self.errors.append(str(error))
        finally:
            with self.lock: self.connections.discard(connection)
            connection.close()

    def accept(self):
        while not self.stopping.is_set():
            try: connection, _ = self.listener.accept()
            except socket.timeout: continue
            except OSError: return
            if self.tls_context is not None:
                try:
                    connection.settimeout(2)
                    connection=self.tls_context.wrap_socket(connection,server_side=True)
                except OSError:
                    connection.close();continue
            with self.lock: self.connections.add(connection)
            thread = threading.Thread(target=self.handle, args=(connection,), daemon=True)
            self.handlers.append(thread); thread.start()

    def __enter__(self): self.thread.start(); return self
    def __exit__(self, *_):
        self.stopping.set(); self.listener.close()
        with self.lock: connections = list(self.connections)
        for connection in connections:
            try: connection.shutdown(socket.SHUT_RDWR)
            except OSError: pass
        self.thread.join(timeout=3)
        for handler in self.handlers: handler.join(timeout=3)
