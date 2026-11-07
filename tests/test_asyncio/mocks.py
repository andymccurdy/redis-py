import asyncio

# Helper Mocking classes for the tests.


class MockStream:
    """
    A class simulating an asyncio input buffer, optionally raising a
    special exception every other read.
    """

    class TestError(BaseException):
        pass

    def __init__(self, data, interrupt_every=0):
        self.data = data
        self.counter = 0
        self.pos = 0
        self.interrupt_every = interrupt_every

    @property
    def _buffer(self):
        # The parsers consult the private StreamReader buffer to decide
        # whether a read can wait (and whether any data is buffered at
        # all); mirror it with the bytes this stream has not consumed yet.
        return bytearray(self.data[self.pos :])

    def tick(self):
        self.counter += 1
        if not self.interrupt_every:
            return
        if (self.counter % self.interrupt_every) == 0:
            raise self.TestError()

    async def read(self, want):
        self.tick()
        want = 5
        result = self.data[self.pos : self.pos + want]
        self.pos += len(result)
        return result

    async def readline(self):
        self.tick()
        find = self.data.find(b"\n", self.pos)
        if find >= 0:
            result = self.data[self.pos : find + 1]
        else:
            result = self.data[self.pos :]
        self.pos += len(result)
        return result

    async def readexactly(self, length):
        self.tick()
        result = self.data[self.pos : self.pos + length]
        if len(result) < length:
            raise asyncio.IncompleteReadError(result, None)
        self.pos += len(result)
        return result
