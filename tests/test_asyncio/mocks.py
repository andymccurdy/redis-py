import asyncio

# Helper Mocking classes for the tests.


class MockStream:
    """
    A class simulating an asyncio input buffer, optionally raising a
    special exception every other read.

    Bytes arrive in stages, like a real StreamReader whose transport
    fills ``_buffer`` over time: ``read``/``readline``/``readexactly``
    move pending bytes into ``_buffer`` as they serve them, and the
    parser's fast paths consume bytes straight out of ``_buffer``.
    ``_buffer`` is a real bytearray, so ``del`` on it consumes the
    bytes, and ``_exception``/``_limit``/``_maybe_resume_transport``
    mirror the StreamReader attributes the fast paths consult.
    """

    class TestError(BaseException):
        pass

    def __init__(self, data, interrupt_every=0):
        self._buffer = bytearray()
        self._pending = bytearray(data)
        self._exception = None
        self._limit = 2**16
        self.counter = 0
        self.interrupt_every = interrupt_every

    def _maybe_resume_transport(self):
        pass

    def at_eof(self):
        return not self._buffer and not self._pending

    def exception(self):
        return self._exception

    def tick(self):
        self.counter += 1
        if not self.interrupt_every:
            return
        if (self.counter % self.interrupt_every) == 0:
            raise self.TestError()

    def _arrive(self, count):
        # Move up to `count` pending bytes into the buffer.
        take = self._pending[:count]
        del self._pending[:count]
        self._buffer += take

    async def read(self, want):
        self.tick()
        # Like a real StreamReader.read(), never return more than
        # `want` bytes; the 5-byte cap below only simulates partial
        # reads. Returning more than asked would let _read_exactly
        # swallow the first bytes of the next frame.
        if not self._buffer:
            self._arrive(min(want, 5))
        result = bytes(self._buffer[:want])
        del self._buffer[: len(result)]
        return result

    async def readline(self):
        self.tick()
        idx = self._buffer.find(b"\n")
        if idx < 0:
            # Move pending bytes until a newline is buffered (or the
            # pending bytes run out), like a transport delivering the
            # line. Only the line itself arrives, so later reads still
            # go through here and can be interrupted.
            nl = self._pending.find(b"\n")
            if nl >= 0:
                self._arrive(nl + 1)
            else:
                self._arrive(len(self._pending))
            idx = self._buffer.find(b"\n")
        if idx >= 0:
            result = bytes(self._buffer[: idx + 1])
            del self._buffer[: idx + 1]
        else:
            result = bytes(self._buffer)
            self._buffer.clear()
        return result

    async def readexactly(self, length):
        self.tick()
        if len(self._buffer) < length:
            self._arrive(length - len(self._buffer))
        if len(self._buffer) < length:
            raise asyncio.IncompleteReadError(bytes(self._buffer), None)
        result = bytes(self._buffer[:length])
        del self._buffer[:length]
        return result
