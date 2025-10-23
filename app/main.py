import logging
import socket
import threading
from collections import namedtuple

logger = logging.getLogger("SERVER")

PORT = 6379
NULL_BULK_STR = "$-1\r\n"
OK_STR = "+OK\r\n"

store: dict[str, str] = {}

Ping = namedtuple("Ping", [])
Echo = namedtuple("Echo", ["msg"])


def main():
    with socket.create_server(("", PORT), reuse_port=True) as server_sock:
        logger.info(f"Started on port {PORT}")

        while True:
            client_sock, addr = server_sock.accept()
            logger.info(f"client with addr {addr[1]} connected.")
            threading.Thread(target=handle_connection, args=(client_sock,)).start()


def handle_connection(sock: socket.socket) -> None:
    while True:
        data = sock.recv(1024)
        if not data:
            break
        logger.info(f"Received: {repr(data.decode())}")
        cmd_list = parse_data(data)
        resp_str = handle_command(cmd_list)
        sock.sendall(resp_str.encode())

    logger.info(f"client {sock.getsockname()[1]} disconnected.")
    sock.close()


# [PING] = *1\r\n$4\r\nPING\r\n
# [ECHO, hey] = *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n


def handle_command(cmd_list: list[str]) -> str:
    assert len(cmd_list) > 0
    cmd = cmd_list[0].upper()
    if cmd == "PING":
        return "+PONG\r\n"
    elif cmd == "ECHO":
        assert len(cmd_list) == 2, "expected value for echo"
        return as_bulk_str(cmd_list[1])
    elif cmd == "SET":
        assert len(cmd_list) == 3, "expected key and value for set"
        key, value = cmd_list[1], cmd_list[2]
        store[key] = value
        logging.info("Updated store with key=%s => value=%s", key, value)
        return OK_STR
    elif cmd == "GET":
        assert len(cmd_list) == 2, "expected key for get"
        key = cmd_list[1]
        value = store.get(key)
        if value:
            return as_bulk_str(value)
        return NULL_BULK_STR
    else:
        logger.error("unexpected command: %s", cmd)
        assert False, "unreachable"


def parse_data(raw_data: bytes) -> list[str]:
    data = raw_data.decode()
    parts = data.strip().split("\r\n")
    logger.debug("raw parts: %s", parts)
    cmd_list: list[str] = []
    for part in parts:
        if part.startswith("*") or part.startswith("$"):
            continue
        cmd_list.append(part)
    logger.debug("parsed cmd list: %s", parts)
    return cmd_list


def as_bulk_str(s: str) -> str:
    return f"${len(s)}\r\n{s}\r\n"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
