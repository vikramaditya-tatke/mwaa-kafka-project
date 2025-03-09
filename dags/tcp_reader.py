import socket
import threading
from queue import Queue
from collections import deque
import time


class TCPLogReader:
    """
    Read logs from a TCP socket.
    Logs are in format - {
        "eventId": 4625,
        "level": "Warning",
        "source": "Security",
        "timestamp": "2025-03-08 23:47:45.358",
        "computer": "SERVER1",
        "user": "ADMIN",
        "message": "Simulated Windows event: Security 4625"
    }
    """

    def __init__(self, host="localhost", port=9999):
        self.host = host
        self.port = port
        self.log_queue = Queue(maxsize=10000)
        self.shutdown_flag = threading.Event()
        self.conn_thread = None
        self._start_connection_handler()

    def _start_connection_handler(self):
        """Start the connection handling thread"""
        self.conn_thread = threading.Thread(
            target=self._connection_handler, daemon=True
        )
        self.conn_thread.start()

    def _connection_handler(self):
        """Manage connection and reconnect attempts"""
        while not self.shutdown_flag.is_set():
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.connect((self.host, self.port))
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    self._receive_data(sock)

            except (ConnectionRefusedError, TimeoutError):
                if not self.shutdown_flag.is_set():
                    time.sleep(1)  # Wait before reconnecting
            except Exception as e:
                if not self.shutdown_flag.is_set():
                    print(f"Connection error: {e}")
                    time.sleep(1)

    def _receive_data(self, sock):
        """Handle incoming data from connected socket"""
        buffer = b""
        while not self.shutdown_flag.is_set():
            try:
                data = sock.recv(65536)
                if not data:
                    break  # Connection closed by remote

                buffer += data
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    self.log_queue.put(line.decode("utf-8", errors="replace").strip())

            except (ConnectionResetError, BrokenPipeError):
                break  # Reconnect will be handled by outer loop
            except Exception as e:
                if not self.shutdown_flag.is_set():
                    print(f"Receive error: {e}")
                break

    def read_logs(self):
        """Generator that yields logs from the queue"""
        try:
            while not self.shutdown_flag.is_set():
                try:
                    yield self.log_queue.get(block=True, timeout=0.1)
                except Exception:
                    continue
        except GeneratorExit:
            self.shutdown()

    def shutdown(self):
        """Clean shutdown of the reader"""
        self.shutdown_flag.set()
        if self.conn_thread:
            self.conn_thread.join(timeout=1)


class HighPerfTCPReader:
    def __init__(self, host="localhost", port=9999):
        self.buffer = bytearray()
        self.lines = deque()
        self.lock = threading.Lock()
        self.cv = threading.Condition()
        self.conn_thread = threading.Thread(
            target=self._connection_loop,
            daemon=True,
        )
        self.conn_thread.start()
        self.host = host
        self.port = port

    def _connection_loop(self):
        backoff = 0.001  # 1ms initial backoff
        while True:
            try:
                with socket.socket() as sock:
                    sock.connect((self.host, self.port))
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    backoff = 0.001  # Reset on success
                    self._read_loop(sock)
            except Exception:
                time.sleep(min(backoff, 5))
                backoff *= 2  # Exponential backoff

    def _read_loop(self, sock):
        buf = memoryview(bytearray(65536))
        while True:
            nbytes = sock.recv_into(buf)
            if nbytes == 0:
                return

            with self.lock:
                self.buffer.extend(buf[:nbytes])
                while True:
                    idx = self.buffer.find(b"\n")
                    if idx == -1:
                        break
                    line = bytes(self.buffer[:idx])
                    self.lines.append(line.decode("utf-8"))
                    del self.buffer[: idx + 1]

            with self.cv:
                self.cv.notify_all()

    def __iter__(self):
        while True:
            with self.lock:
                if self.lines:
                    yield self.lines.popleft()
                    continue

            with self.cv:
                self.cv.wait(0.1)  # Event-driven wait
