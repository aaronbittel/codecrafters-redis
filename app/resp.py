from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import IO, Any, Protocol, Self


@dataclass
class Command:
    name: str
    args: list[str]

    @classmethod
    def parse(cls, reader: IO[bytes]) -> Self:
        arr_prefix = _safe_read(reader, 1)
        if arr_prefix != b"*":
            raise ValueError(f"Expected array for command, bot got {arr_prefix!r}")
        try:
            count = int(_safe_readline(reader).strip())
        except ValueError as v:
            raise ValueError("Protocol error: expected integer") from v
        raw_args: list[bytes] = []
        for _ in range(count):
            prefix = _safe_read(reader, 1)
            if prefix != b"$":
                raise ValueError(
                    f"Expected bulk string for element, but got {prefix!r}"
                )
            try:
                length = int(_safe_readline(reader).strip())
            except ValueError as v:
                raise ValueError("Protocol error: expected integer") from v
            arg = _safe_readline(reader).strip()
            assert len(arg) == length, (
                f"promised length was {length}, but got {arg} with length {len(arg)}"
            )
            raw_args.append(arg)
        args = list(map(lambda arg: arg.decode(), raw_args))
        return cls(name=args[0].upper(), args=args[1:])


@dataclass
class Stream:
    values: list[StreamValue] = field(default_factory=list)

    def append(self, id_str: str, values: dict[str, str]) -> None:
        given_id = StreamID.from_str(id_str)
        resolved_id = (
            self._resolve_id(given_id) if given_id.sequence_number is None else given_id
        )
        assert resolved_id.milliseconds_time is not None
        assert resolved_id.sequence_number is not None
        if resolved_id == StreamID(0, 0):
            raise ValueError("The ID specified in XADD must be greater than 0-0")
        if len(self.values) >= 1 and self.values[-1].id >= resolved_id:
            raise ValueError(
                "The ID specified in XADD is equal or smaller than the target stream top item"
            )
        self.values.append(StreamValue(id=resolved_id, values=values))

    def _resolve_id(self, given_id: StreamID) -> StreamID:
        last_id = self.values[-1].id if len(self.values) > 0 else None
        if given_id.milliseconds_time == 0:
            given_id.sequence_number = 1
        elif len(self.values) == 0:
            given_id.sequence_number = 0
        elif last_id and last_id.milliseconds_time == given_id.milliseconds_time:
            given_id.sequence_number = last_id.sequence_number + 1
        else:
            given_id.sequence_number = 0
        return given_id

    # TODO: need to resolve id for xrange as well?
    def __getitem__(
        self, key: int | slice[StreamID]
    ) -> list[StreamValue] | StreamValue:
        if isinstance(key, slice):
            stop = key.stop
            if (
                stop is None
                or stop.milliseconds_time is None
                and stop.sequence_number is None
            ):
                stop = self.values[-1].id
            return list(
                filter(lambda value: key.start <= value.id <= stop, self.values)
            )
        return self.values[key]

    def __len__(self) -> int:
        return len(self.values)


@dataclass
class StreamValue:
    id: StreamID
    values: dict[str, str] = field(default_factory=dict)


@dataclass(order=True)
class StreamID:
    milliseconds_time: int | None
    sequence_number: int | None

    @classmethod
    def from_str(cls, s: str) -> Self:
        if s == "*":
            return cls(int(time.time() * 1000), None)
        millis, seq = s.split("-", maxsplit=1)
        millis = int(millis)
        if seq == "*":
            return cls(milliseconds_time=millis, sequence_number=None)
        return cls(milliseconds_time=millis, sequence_number=int(seq))

    @classmethod
    def from_str_xrange(cls, s: str, *, start: bool) -> Self:
        """start: specifies if is it the start index or the end index."""
        if s == "-":
            assert start == True
            return cls(0, 0)
        if s == "+":
            assert start == False
            return cls(None, None)
        if "-" in s:
            millis, seq = map(int, s.split("-", maxsplit=1))
            return cls(millis, seq)
        millis = int(s)
        seq = 0 if start else None
        return cls(millis, seq)

    def __str__(self) -> str:
        return f"{self.milliseconds_time}-{self.sequence_number}"


def dump_resp(resp: str) -> None:
    dump = " ".join(
        filter(
            lambda p: not (p.startswith("*") or p.startswith("$")),
            resp.split("\r\n"),
        )
    )
    print(dump if dump.strip() != "" else "(empty)")


type RedisPrimitive = int | str
type RedisValue = SimpleString | SimpleError | Integer | BulkString | Array


class RedisEncodable(Protocol):
    def encode(self) -> bytes: ...


@dataclass
class SimpleString(RedisEncodable):
    value: str

    def encode(self) -> bytes:
        return f"+{self.value}\r\n".encode()


@dataclass
class SimpleError(RedisEncodable):
    msg: str

    def encode(self) -> bytes:
        return f"-ERR {self.msg}\r\n".encode()


@dataclass
class Integer(RedisEncodable):
    value: int

    def encode(self) -> bytes:
        return f":{self.value}\r\n".encode()


@dataclass
class BulkString(RedisEncodable):
    value: str | None

    def encode(self) -> bytes:
        if self.value is None:
            return b"$-1\r\n"
        return f"${len(self.value)}\r\n{self.value}\r\n".encode()


@dataclass
class Array(RedisEncodable):
    values: list[RedisValue | RedisPrimitive] | None

    def encode(self) -> bytes:
        if self.values is None:
            return b"*-1\r\n"
        out = f"*{len(self.values)}\r\n".encode()
        for value in self.values:
            if isinstance(value, str):
                value = BulkString(value)
            elif isinstance(value, int):
                value = Integer(value)
            out += value.encode()
        return out


@dataclass
class Null(RedisEncodable):
    def encode(self) -> bytes:
        return b"_\r\n"


def to_redis_value(value: list[Any]) -> RedisEncodable:
    if isinstance(value, str):
        return BulkString(value)
    elif isinstance(value, list):
        return Array([to_redis_value(val) for val in value])
    else:
        raise TypeError(value)


RESP_OK = SimpleString("OK")
EMPTY_ARRAY = Array([])
NULL_ARRAY = Array(None)


def _safe_read(reader: IO[bytes], size: int) -> bytes:
    data = reader.read(size)
    if not data:
        raise ConnectionResetError("client closed connection")
    return data


def _safe_readline(reader: IO[bytes], limit: int = -1) -> bytes:
    line = reader.readline(limit)
    if not line:
        raise ConnectionResetError("client closed connection")
    return line
