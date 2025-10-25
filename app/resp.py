from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from io import BufferedReader
from typing import IO, Self

NULL_BULK_BYTES = b"$-1\r\n"
OK_BYTES = b"+OK\r\n"
EMPTY_ARRAY_BYTES = b"*0\r\n"
NULL_ARRAY_BYTES = b"*-1\r\n"


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

    def __getitem__(self, i: int) -> StreamValue:
        return self.values[i]

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

    def __str__(self) -> str:
        return f"{self.milliseconds_time}-{self.sequence_number}"


def create_command(*args: str) -> str:
    def fmt(arg: str) -> str:
        return f"${len(arg)}\r\n{arg}\r\n"

    return f"*{len(args)}\r\n{''.join(fmt(arg) for arg in args)}"


def dump_resp(resp: str) -> None:
    dump = " ".join(
        filter(
            lambda p: not (p.startswith("*") or p.startswith("$")),
            resp.split("\r\n"),
        )
    )
    print(dump if dump.strip() != "" else "(empty)")


def as_bulk_bytes(s: str) -> bytes:
    return f"${len(s)}\r\n{s}\r\n".encode()


def as_integer_bytes(n: int) -> bytes:
    return f":{n}\r\n".encode()


def as_array_bytes(xs: list[str]) -> bytes:
    def fmt(arg: str) -> str:
        return f"${len(arg)}\r\n{arg}\r\n"

    return f"*{len(xs)}\r\n{''.join(fmt(x) for x in xs)}".encode()


def as_error_bytes(msg: str) -> bytes:
    return f"-ERR {msg}\r\n".encode()


def as_simple_string_bytes(s: str) -> bytes:
    return f"+{s}\r\n".encode()


def _safe_read(reader: BufferedReader, size: int) -> bytes:
    data = reader.read(size)
    if not data:
        raise ConnectionResetError("client closed connection")
    return data


def _safe_readline(reader: BufferedReader, size: int | None = -1) -> bytes:
    line = reader.readline(size)
    if not line:
        raise ConnectionResetError("client closed connection")
    return line
