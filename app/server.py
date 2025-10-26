import argparse
import logging
import socket
import threading
import time

from app import resp
from app.config import PORT
from app.resp import (
    EMPTY_ARRAY,
    Array,
    BulkString,
    Command,
    Integer,
    RedisValue,
    SimpleError,
    SimpleString,
    Stream,
    StreamID,
    to_redis_value,
)

logger = logging.getLogger("SERVER")


class Server:
    def __init__(self, host: str = "localhost", port: int = PORT) -> None:
        self.host = host
        self.port = port
        # TODO: Improve this
        self.store: dict[str, str | list[str] | resp.Stream] = {}
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
            resp = self.handle_command(cmd).encode()
            sock.sendall(resp)

        sock.close()

    def handle_command(self, cmd: Command) -> RedisValue:
        if cmd.name == "PING":
            return SimpleString("PONG")
        elif cmd.name == "ECHO":
            if len(cmd.args) != 1 or cmd.args[0] == "":
                return SimpleError("ECHO cmd: wrong number of arguments")
            return BulkString(cmd.args[0])
        elif cmd.name == "SET":
            if len(cmd.args) < 2:
                return SimpleError("SET cmd: expected key and value")
            key, value, *options = cmd.args
            self.store[key] = value
            for i, opt in enumerate(options):
                if opt.upper() == "PX":
                    if len(options) <= i + 1:
                        return SimpleError(
                            "SET cmd: expected millis value for px option"
                        )
                    try:
                        ttl = int(options[i + 1])
                    except ValueError:
                        return SimpleError("SET cmd: PX option must be an integer")
                    threading.Timer(
                        ttl / 1000, function=self.store.pop, args=(key,)
                    ).start()
            return resp.RESP_OK
        elif cmd.name == "GET":
            if len(cmd.args) != 1:
                return SimpleError("GET cmd: expected key")
            key = cmd.args[0]
            value = self.store.get(key)
            if value:
                return BulkString(value)
            return BulkString(None)
        elif cmd.name == "RPUSH":
            if len(cmd.args) < 2:
                return SimpleError("RPUSH cmd: expected key and value")
            key, *values = cmd.args
            if key not in self.store:
                self.store[key] = []
            assert isinstance(self.store[key], list), (
                f"RPUSH cmd: expected {self.store[key]} to be a list"
            )
            for value in values:
                self.store[key].append(value)
            return Integer(len(self.store[key]))
        elif cmd.name == "LRANGE":
            if len(cmd.args) != 3:
                return SimpleError("LRANGE cmd: expected key, start, end")
            try:
                key, start_str, end_str = (
                    cmd.args[0],
                    int(cmd.args[1]),
                    int(cmd.args[2]),
                )
            except ValueError:
                return SimpleError("LRANGE cmd: expected integer for start, end")
            li = self.store.get(key)
            if li is None:
                return resp.EMPTY_ARRAY
            assert isinstance(li, list), f"LRANGE cmd: expected {li} to be a list"
            if start_str < 0:
                start_str = len(li) + start_str if start_str >= -len(li) else 0
            end_str = min(end_str if end_str >= 0 else len(li) + end_str, len(li) - 1)
            if start_str >= len(li) or start_str > end_str:
                return resp.EMPTY_ARRAY
            return Array(li[start_str : end_str + 1])
        elif cmd.name == "LPUSH":
            if len(cmd.args) < 2:
                return SimpleError("LPUSH cmd: expected key and value")
            key, *values = cmd.args
            if key not in self.store:
                self.store[key] = []
            assert isinstance(self.store[key], list), (
                f"LPUSH cmd: expected {self.store[key]} to be a list"
            )
            for value in values:
                self.store[key].insert(0, value)
            return Integer(len(self.store[key]))
        elif cmd.name == "LLEN":
            if len(cmd.args) != 1:
                return SimpleError("LLEN cmd: expected key")
            key = cmd.args[0]
            return Integer(len(self.store.get(key, [])))
        elif cmd.name == "LPOP":
            if len(cmd.args) < 1:
                return SimpleError("LPOP cmd: expected key")
            key = cmd.args[0]
            li = self.store.get(key)
            if li is None:
                return resp.NULL_BULK
            assert isinstance(li, list), f"LPOP cmd: expected {li} to be a list"
            if len(li) == 0:
                return resp.NULL_BULK
            if len(cmd.args) == 1:
                item = li.pop(0)
                return BulkString(item)
            try:
                count = int(cmd.args[1])
            except ValueError:
                return SimpleError("LPOP cmd: expected integer for count")
            count = count if count <= len(li) else len(li)
            return Array([li.pop(0) for _ in range(count)])
        elif cmd.name == "BLPOP":
            if len(cmd.args) != 2:
                return SimpleError("BLPOP cmd: expected key and timeout")
            try:
                key, timeout = cmd.args[0], float(cmd.args[1])
            except ValueError:
                return SimpleError("BLPOP cmd: expected number for timeout")
            item: str | None = None
            sleep_time_s = 50 / 1000
            cur_time = 0.0
            while True:
                item = self._get_item(key)
                if item is not None:
                    return Array([key, item])
                elif timeout != 0 and cur_time >= timeout:
                    # timeout hit
                    return resp.NULL_ARRAY
                cur_time += sleep_time_s
                time.sleep(sleep_time_s)
        elif cmd.name == "TYPE":
            if len(cmd.args) != 1:
                return SimpleError("TYPE cmd: expected key")
            key = cmd.args[0]
            typ = ""
            if key not in self.store:
                typ = "none"
            elif isinstance(self.store[key], resp.Stream):
                typ = "stream"
            else:
                typ = "string"
            return SimpleString(typ)
        elif cmd.name == "XADD":
            # NOTE: What to do if no key-value pairs a given?
            if len(cmd.args) <= 2:
                return SimpleError("XADD cmd: expected key, id")
            key, id_str, *values = cmd.args
            if key not in self.store:
                self.store[key] = resp.Stream()
            elif not isinstance(self.store[key], resp.Stream):
                # TODO: Add this to the other commands as well
                return SimpleError(
                    "XADD cmd: WRONGTYPE Operation against a key holding the wrong kind of value"
                )
            if len(values) % 2 != 0:
                return SimpleError("XADD cmd: no value given for key")

            stream = self.store[key]
            assert isinstance(stream, resp.Stream), (
                f"expected {stream} to be a resp.Stream"
            )
            try:
                stream.append(id_str, dict(zip(values[::2], values[1::2])))
            except ValueError as v:
                return SimpleError(v)
            return BulkString(str(stream[-1].id))
        elif cmd.name == "XRANGE":
            if len(cmd.args) != 3:
                return SimpleError("XRANGE cmd: expected key, start, end")
            key, start_str, end_str = cmd.args
            if key not in self.store:
                return EMPTY_ARRAY
            stream = self.store[key]
            if not isinstance(stream, resp.Stream):
                return SimpleError(
                    "XRANGE cmd: WRONGTYPE Operation against a key holding the wrong kind of value"
                )
            start_id = StreamID.from_str_xrange(start_str, start=True)
            end_id = StreamID.from_str_xrange(end_str, start=False)
            logger.info("start_id=%s", start_id)
            logger.info("end_id=%s", end_id)
            stream_range = stream[start_id:end_id]
            assert isinstance(stream_range, list), (
                f"XRANGE cmd: expected {stream_range} to be a list"
            )
            logger.info("stream_range=%s", stream_range)
            res = []
            for entry in stream_range:
                li = []
                li.append(str(entry.id))
                inner = []
                for key, value in entry.values.items():
                    inner.append(key)
                    inner.append(value)
                li.append(inner)
                res.append(li)
            logger.info("res=%s", res)
            return to_redis_value(res)
        elif cmd.name == "XREAD":
            # NOTE: currently only 1 stream allowed
            if len(cmd.args) != 3:
                return SimpleError("XREAD cmd: expected STREAMS, from")
            streams_keyword, key, from_exclusive = cmd.args
            if streams_keyword.upper() != "STREAMS":
                return SimpleError(
                    f"XREAD cmd: expected STREAMS keyword, but got {streams_keyword}"
                )
            start_id = StreamID.from_str(from_exclusive)
            logger.info("start_id=%s", start_id)
            # NOTE: Make sure that given id is exclusive
            start_id.sequence_number += 1
            logger.info("exclusive: start_id=%s", start_id)
            if key not in self.store:
                return SimpleError(f"XREAD cmd: no stream stored with {key}")
            stream = self.store[key]
            if not isinstance(stream, Stream):
                return SimpleError(
                    "XREAD cmd: WRONGTYPE Operation against a key holding the wrong kind of value"
                )
            stream_range = stream[start_id:]
            assert isinstance(stream_range, list)
            outer = []
            per_streamkey = [key]

            stream_values = []
            for val in stream_range:
                li = [str(val.id)]
                inner = []
                for k, v in val.values.items():
                    inner.append(k)
                    inner.append(v)
                li.append(inner)
                stream_values.append(li)

            per_streamkey.append(stream_values)
            outer.append(per_streamkey)
            return to_redis_value(outer)
        else:
            logger.error("unexpected command: %s", cmd)
            return SimpleError(f"unknown command {cmd.name}")

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
