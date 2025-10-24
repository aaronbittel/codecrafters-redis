import argparse
import logging
import socket
import threading
import time

from app import resp
from app.config import PORT
from app.resp import (
    Command,
    as_array_bytes,
    as_bulk_bytes,
    as_error_bytes,
    as_integer_bytes,
)

logger = logging.getLogger("SERVER")


class Server:
    def __init__(self, host: str = "localhost", port: int = PORT) -> None:
        self.host = host
        self.port = port
        self.store: dict[str, str | list[str]] = {}
        self.stop_event = threading.Event()

    def start(self) -> None:
        with socket.create_server(
            (self.host, self.port), reuse_port=True
        ) as server_sock:
            logger.info("Started on port %d", self.port)

            server_sock.settimeout(0.1)

            while not self.stop_event.is_set():
                try:
                    client_sock, addr = server_sock.accept()
                except socket.timeout:
                    continue
                logger.info("client with addr %d connected.", addr[1])
                threading.Thread(
                    target=self.handle_connection, args=(client_sock,)
                ).start()

    def handle_connection(self, sock: socket.socket) -> None:
        while True:
            try:
                cmd = Command.parse(sock.makefile("rb"))
            except ValueError as v:
                msg = f"-ERR {v}\r\n"
                sock.sendall(msg.encode())
                continue
            except ConnectionResetError:
                logger.info("client %d disconnected.", sock.getsockname()[1])
                break
            logger.info("parsed cmd: %s", cmd)
            sock.sendall(self.handle_command(cmd))

        sock.close()

    def handle_command(self, cmd: Command) -> bytes:
        if cmd.name == "PING":
            return b"+PONG\r\n"
        elif cmd.name == "ECHO":
            if len(cmd.args) != 1 or cmd.args[0] == "":
                return as_error_bytes("ECHO cmd: wrong number of arguments")
            return as_bulk_bytes(cmd.args[0])
        elif cmd.name == "SET":
            if len(cmd.args) < 2:
                return as_error_bytes("SET cmd: expected key and value")
            key, value, *options = cmd.args
            self.store[key] = value
            for i, opt in enumerate(options):
                if opt.upper() == "PX":
                    if len(options) <= i + 1:
                        return as_error_bytes(
                            "SET cmd: expected millis value for px option"
                        )
                    try:
                        ttl = int(options[i + 1])
                    except ValueError:
                        return as_error_bytes("SET cmd: PX option must be an integer")
                    threading.Timer(
                        ttl / 1000, function=self.store.pop, args=(key,)
                    ).start()
            return resp.OK_BYTES
        elif cmd.name == "GET":
            if len(cmd.args) != 1:
                return as_error_bytes("GET cmd: expected key")
            key = cmd.args[0]
            value = self.store.get(key)
            if value:
                return as_bulk_bytes(value)
            return resp.NULL_BULK_BYTES
        elif cmd.name == "RPUSH":
            if len(cmd.args) < 2:
                return as_error_bytes("RPUSH cmd: expected key and value")
            key, *values = cmd.args
            if key not in self.store:
                self.store[key] = []
            assert isinstance(self.store[key], list), (
                f"RPUSH cmd: expected {self.store[key]} to be a list"
            )
            for value in values:
                self.store[key].append(value)
            return as_integer_bytes(len(self.store[key]))
        elif cmd.name == "LRANGE":
            if len(cmd.args) != 3:
                return as_error_bytes("LRANGE cmd: expected key, start, end")
            try:
                key, start, end = cmd.args[0], int(cmd.args[1]), int(cmd.args[2])
            except ValueError:
                return as_error_bytes("LRANGE cmd: expected integer for start, end")
            li = self.store.get(key)
            if li is None:
                return resp.EMPTY_ARRAY_BYTES
            assert isinstance(li, list), f"LRANGE cmd: expected {li} to be a list"
            if start < 0:
                start = len(li) + start if start >= -len(li) else 0
            end = min(end if end >= 0 else len(li) + end, len(li) - 1)
            if start >= len(li) or start > end:
                return resp.EMPTY_ARRAY_BYTES
            return as_array_bytes(li[start : end + 1])
        elif cmd.name == "LPUSH":
            if len(cmd.args) < 2:
                return as_error_bytes("LPUSH cmd: expected key and value")
            key, *values = cmd.args
            if key not in self.store:
                self.store[key] = []
            assert isinstance(self.store[key], list), (
                f"LPUSH cmd: expected {self.store[key]} to be a list"
            )
            for value in values:
                self.store[key].insert(0, value)
            return as_integer_bytes(len(self.store[key]))
        elif cmd.name == "LLEN":
            if len(cmd.args) != 1:
                return as_error_bytes("LLEN cmd: expected key")
            key = cmd.args[0]
            return as_integer_bytes(len(self.store.get(key, [])))
        elif cmd.name == "LPOP":
            if len(cmd.args) < 1:
                return as_error_bytes("LPOP cmd: expected key")
            key = cmd.args[0]
            li = self.store.get(key)
            if li is None:
                return resp.NULL_BULK_BYTES
            assert isinstance(li, list), f"LPOP cmd: expected {li} to be a list"
            if len(li) == 0:
                return resp.NULL_BULK_BYTES
            if len(cmd.args) == 1:
                item = li.pop(0)
                return as_bulk_bytes(item)
            try:
                count = int(cmd.args[1])
            except ValueError:
                return as_error_bytes("LPOP cmd: expected integer for count")
            count = count if count <= len(li) else len(li)
            return as_array_bytes([li.pop(0) for _ in range(count)])
        elif cmd.name == "BLPOP":
            if len(cmd.args) != 2:
                return as_error_bytes("BLPOP cmd: expected key and timeout")
            try:
                key, timeout = cmd.args[0], float(cmd.args[1])
            except ValueError:
                return as_error_bytes("BLPOP cmd: expected number for timeout")
            item: str | None = None
            sleep_time_s = 50 / 1000
            cur_time = 0.0
            while True:
                item = self._get_item(key)
                if item is not None:
                    return as_array_bytes([key, item])
                elif timeout != 0 and cur_time >= timeout:
                    # timeout hit
                    return resp.NULL_ARRAY_BYTES
                cur_time += sleep_time_s
                time.sleep(sleep_time_s)
        else:
            logger.error("unexpected command: %s", cmd)
            return as_error_bytes(f"unknown command {cmd.name}")

    def _get_item(self, key: str) -> str | None:
        li = self.store.get(key)
        if li is None:
            return None
        assert isinstance(li, list), f"BLPOP cmd: expected {li} to be a list"
        if len(li) > 0:
            return li.pop(0)
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser("Redis-Server")
    parser.add_argument(
        "-p", "--port", type=int, default=PORT, help="port the server runs on"
    )
    args = parser.parse_args()
    server = Server(port=args.port)
    server.start()
