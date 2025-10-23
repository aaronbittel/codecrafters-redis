import logging
import socket
import threading
import time
from collections import namedtuple

logger = logging.getLogger("SERVER")

PORT = 6379

RESP_NULL_BULK_STR = "$-1\r\n"
RESP_OK_STR = "+OK\r\n"
RESP_EMPTY_ARRAY = "*0\r\n"

store: dict[str, str | list[str]] = {}

Ping = namedtuple("Ping", [])
Echo = namedtuple("Echo", ["msg"])


def main():
    with socket.create_server(("", PORT), reuse_port=True) as server_sock:
        logger.info("Started on port %d", PORT)

        while True:
            client_sock, addr = server_sock.accept()
            logger.info("client with addr %d connected.", addr[1])
            threading.Thread(target=handle_connection, args=(client_sock,)).start()


def handle_connection(sock: socket.socket) -> None:
    while True:
        data = sock.recv(1024)
        if not data:
            break
        logger.debug("Received: %s", {repr(data.decode())})
        cmd_list = parse_data(data)
        logger.info("parsed cmd_list: %s", cmd_list)
        resp_str = handle_command(cmd_list)
        sock.sendall(resp_str.encode())

    logger.info("client %d disconnected.", sock.getsockname()[1])
    sock.close()


def handle_command(cmd_list: list[str]) -> str:
    assert len(cmd_list) > 0
    cmd, args = cmd_list[0].upper(), cmd_list[1:]
    if cmd == "PING":
        return "+PONG\r\n"
    elif cmd == "ECHO":
        assert len(args) == 1, "ECHO cmd: expected value"
        return as_bulk_str(args[0])
    elif cmd == "SET":
        assert len(args) >= 2, "SET cmd: expected key and value"
        key, value, *options = args
        store[key] = value
        logger.info("Updated store with key=%s => value=%s", key, value)
        for i, opt in enumerate(options):
            if opt.upper() == "PX":
                assert len(options) > i + 1, "expected millis value for px option"
                # TODO: check options[i+1] first
                ttl = int(options[i + 1])
                threading.Timer(ttl / 1000, function=store.pop, args=(key,)).start()
        return RESP_OK_STR
    elif cmd == "GET":
        assert len(args) == 1, "GET cmd: expected key"
        key = args[0]
        value = store.get(key)
        if value:
            return as_bulk_str(value)
        return RESP_NULL_BULK_STR
    elif cmd == "RPUSH":
        assert len(args) >= 2, "RPUSH cmd: expected key and value"
        key, *values = args
        if key not in store:
            store[key] = []
        assert isinstance(store[key], list), (
            f"expected {store[key]} to be a list for rpush cmd"
        )
        for value in values:
            store[key].append(value)
        return as_integer_str(len(store[key]))
    elif cmd == "LRANGE":
        assert len(args) == 3, "LRANGE cmd: expected key, start, end"
        # TODO: Check start, end for integer
        key, start, end = args[0], int(args[1]), int(args[2])
        li = store.get(key)
        if li is None:
            return RESP_EMPTY_ARRAY
        assert isinstance(li, list), f"LRANGE cmd: expected {li} to be a list"
        if start < 0:
            start = len(li) + start if start >= -len(li) else 0
        end = min(end if end >= 0 else len(li) + end, len(li) - 1)
        if start >= len(li) or start > end:
            return RESP_EMPTY_ARRAY
        return as_array_str(li[start : end + 1])
    else:
        logger.error("unexpected command: %s", cmd)
        assert False, "unreachable"


def parse_data(raw_data: bytes) -> list[str]:
    def relevant(s: str) -> bool:
        return not (s.startswith("*") or s.startswith("$"))

    data = raw_data.decode()
    parts = data.strip().split("\r\n")
    return [part for part in parts if relevant(part)]


def as_bulk_str(s: str) -> str:
    return f"${len(s)}\r\n{s}\r\n"


def as_integer_str(n: int) -> str:
    return f":{n}\r\n"


def as_array_str(xs: list[str]) -> str:
    def fmt(arg: str) -> str:
        return f"${len(arg)}\r\n{arg}\r\n"

    return f"*{len(xs)}\r\n{''.join(fmt(x) for x in xs)}"


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
