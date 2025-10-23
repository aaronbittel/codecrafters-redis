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
RESP_NULL_ARRAY = "*-1\r\n"

store: dict[str, str | list[str]] = {}


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
            f"RPUSH cmd: expected {store[key]} to be a list"
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
    elif cmd == "LPUSH":
        assert len(args) >= 2, "LPUSH cmd: expected key and value"
        key, *values = args
        if key not in store:
            store[key] = []
        assert isinstance(store[key], list), (
            f"LPUSH cmd: expected {store[key]} to be a list"
        )
        for value in values:
            store[key].insert(0, value)
        return as_integer_str(len(store[key]))
    elif cmd == "LLEN":
        assert len(args) == 1, "LLEN cmd: expected key"
        key = args[0]
        return as_integer_str(len(store.get(key, [])))
    elif cmd == "LPOP":
        assert len(args) >= 1, "LPOP cmd: expected key"
        key = args[0]
        li = store.get(key)
        if li is None:
            return RESP_NULL_BULK_STR
        assert isinstance(li, list), f"LPOP cmd: expected {li} to be a list"
        if len(li) == 0:
            return RESP_NULL_BULK_STR
        if len(args) == 1:
            item = li.pop(0)
            return as_bulk_str(item)
        # TODO: Check args[1] for integer
        count = int(args[1])
        count = count if count <= len(li) else len(li)
        return as_array_str([li.pop(0) for _ in range(count)])
    elif cmd == "BLPOP":
        assert len(args) == 2, "BLPOP cmd: expected key and timeout"
        # TODO: Check args[1] for integer
        key, timeout = args[0], float(args[1])
        item: str | None = None
        sleep_time_s = 50 / 1000
        cur_time = 0.0
        while True:
            item = get_item(key)
            if item is not None:
                return as_array_str([key, item])
            elif timeout != 0 and cur_time >= timeout:
                # timeout hit
                return RESP_NULL_ARRAY
            cur_time += sleep_time_s
            time.sleep(sleep_time_s)
    else:
        logger.error("unexpected command: %s", cmd)
        assert False, "unreachable"


def get_item(key: str) -> str | None:
    li = store.get(key)
    if li is None:
        return None
    assert isinstance(li, list), f"BLPOP cmd: expected {li} to be a list"
    if len(li) > 0:
        return li.pop(0)
    return None


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
