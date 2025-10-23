import logging
import socket
import threading
import time
from collections import namedtuple

logger = logging.getLogger("SERVER")

PORT = 6379
NULL_BULK_STR = "$-1\r\n"
OK_STR = "+OK\r\n"

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
        logger.info("Received: %s", {repr(data.decode())})
        cmd_list = parse_data(data)
        logger.info("parsed cmd_list: %s", cmd_list)
        resp_str = handle_command(cmd_list)
        sock.sendall(resp_str.encode())

    logger.info("client %d disconnected.", sock.getsockname()[1])
    sock.close()


# [PING] = *1\r\n$4\r\nPING\r\n
# [ECHO, hey] = *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n


def handle_command(cmd_list: list[str]) -> str:
    assert len(cmd_list) > 0
    cmd = cmd_list[0].upper()
    if cmd == "PING":
        return "+PONG\r\n"
    elif cmd == "ECHO":
        assert len(cmd_list) == 2, "expected value for echo cmd"
        return as_bulk_str(cmd_list[1])
    elif cmd == "SET":
        assert len(cmd_list) >= 3, "expected key and value for set cmd"
        _, key, value, *options = cmd_list
        store[key] = value
        logger.info("Updated store with key=%s => value=%s", key, value)
        for i, opt in enumerate(options):
            if opt.upper() == "PX":
                assert len(options) > i + 1, "expected millis value for px option"
                # TODO: check opt[i+1] first
                ttl = int(options[i + 1])
                threading.Timer(ttl / 1000, function=store.pop, args=(key,)).start()
        return OK_STR
    elif cmd == "GET":
        assert len(cmd_list) == 2, "expected key for get cmd"
        key = cmd_list[1]
        value = store.get(key)
        if value:
            return as_bulk_str(value)
        return NULL_BULK_STR
    elif cmd == "RPUSH":
        assert len(cmd_list) == 3, "expected key and value for rpush cmd"
        key, value = cmd_list[1], cmd_list[2]
        li = store.get(key)
        if li:
            assert isinstance(li, list), f"expected {li} to be a list for rpush cmd"
            store[key].append(value)
        else:
            store[key] = [value]
        return as_integer_str(len(store[key]))
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


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
