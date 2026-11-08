"""Disposable authenticated HTTP CONNECT proxy with explicit outage controls."""
import base64
import socket
import threading


class ConnectProxy:
    def __init__(self, upstream, username='v-test', password='disposable-proxy-password', observe_client_bytes=None):
        self.upstream = upstream
        self.observe_client_bytes = observe_client_bytes
        self.authorization = 'Basic ' + base64.b64encode(f'{username}:{password}'.encode()).decode()
        self.listener = socket.socket()
        self.listener.bind(('127.0.0.1', 0))
        self.listener.listen()
        self.listener.settimeout(.2)
        self.address = '127.0.0.1:' + str(self.listener.getsockname()[1])
        self.stopping = threading.Event()
        self.unavailable = threading.Event()
        self.lock = threading.Lock()
        self.sockets = set()
        self.handlers = []
        self.accepted = 0
        self.rejected = 0
        self.thread = threading.Thread(target=self.accept, daemon=True)

    @staticmethod
    def close_socket(connection):
        try: connection.shutdown(socket.SHUT_RDWR)
        except OSError: pass
        connection.close()

    def pause(self):
        self.unavailable.set()
        with self.lock:
            for connection in list(self.sockets): self.close_socket(connection)

    def resume(self):
        self.unavailable.clear()

    def handle(self, raw):
        connections = [raw]
        try:
            raw.settimeout(5)
            request = b''
            while b'\r\n\r\n' not in request:
                part = raw.recv(1)
                if not part: return
                request += part
                if len(request) > 16384: return
            lines = request.decode('ascii').split('\r\n')
            assert lines[0].startswith('CONNECT '), 'expected CONNECT request'
            headers = {key.lower(): value.strip() for key, value in (line.split(':', 1) for line in lines[1:] if line)}
            if headers.get('proxy-authorization') != self.authorization:
                self.rejected += 1
                raw.sendall(b'HTTP/1.1 407 Proxy Authentication Required\r\nContent-Length: 0\r\n\r\n')
                return
            if self.unavailable.is_set():
                raw.sendall(b'HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n')
                return
            host, port = self.upstream.rsplit(':', 1)
            upstream = socket.create_connection((host, int(port)), timeout=5)
            upstream.settimeout(None)
            connections.append(upstream)
            with self.lock:
                self.sockets.add(upstream)
                if self.unavailable.is_set(): return
                self.accepted += 1
            raw.sendall(b'HTTP/1.1 200 Connection Established\r\n\r\n')
            raw.settimeout(None)

            def forward(source, destination):
                try:
                    while not self.stopping.is_set():
                        value = source.recv(65536)
                        if not value: break
                        destination.sendall(value)
                        if source is raw and self.observe_client_bytes is not None:
                            self.observe_client_bytes(value)
                except OSError: pass
                finally:
                    try: destination.shutdown(socket.SHUT_RDWR)
                    except OSError: pass
            outgoing = threading.Thread(target=forward, args=(raw, upstream), daemon=True)
            outgoing.start()
            forward(upstream, raw)
            self.close_socket(raw)
            outgoing.join(timeout=2)
        except (OSError, UnicodeError): pass
        finally:
            with self.lock:
                for connection in connections:
                    self.sockets.discard(connection)
                    self.close_socket(connection)

    def accept(self):
        while not self.stopping.is_set():
            try: raw, _ = self.listener.accept()
            except socket.timeout: continue
            except OSError: return
            with self.lock: self.sockets.add(raw)
            handler = threading.Thread(target=self.handle, args=(raw,), daemon=True)
            self.handlers.append(handler)
            handler.start()

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stopping.set()
        self.listener.close()
        with self.lock:
            for connection in list(self.sockets): self.close_socket(connection)
        self.thread.join(timeout=3)
        for handler in self.handlers: handler.join(timeout=3)
