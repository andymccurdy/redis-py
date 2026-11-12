from base import Benchmark

from redis.connection import SYM_CRLF, SYM_EMPTY, Connection


class StringJoiningConnection(Connection):
    def send_packed_command(self, command, check_health=True):
        "Send an already packed command to the Redis server"
        if not self._sock:
            self.connect()
        try:
            self._sock.sendall(command)
        except OSError as e:
            self.disconnect()
            if len(e.args) == 1:
                _errno, errmsg = "UNKNOWN", e.args[0]
            else:
                _errno, errmsg = e.args
            raise ConnectionError(f"Error {_errno} while writing to socket. {errmsg}.")
        except Exception:
            self.disconnect()
            raise

    def pack_command(self, *args):
        "Pack a series of arguments into a value Redis command"
        args_output = SYM_EMPTY.join(
            [b"$%d\r\n%s\r\n" % (len(k), k) for k in map(self.encoder.encode, args)]
        )
        output = b"*%d\r\n%s" % (len(args), args_output)
        return output


class ListJoiningConnection(Connection):
    def send_packed_command(self, command, check_health=True):
        if not self._sock:
            self.connect()
        try:
            if isinstance(command, str):
                command = [command]
            for item in command:
                self._sock.sendall(item)
        except OSError as e:
            self.disconnect()
            if len(e.args) == 1:
                _errno, errmsg = "UNKNOWN", e.args[0]
            else:
                _errno, errmsg = e.args
            raise ConnectionError(f"Error {_errno} while writing to socket. {errmsg}.")
        except Exception:
            self.disconnect()
            raise

    def pack_command(self, *args):
        output = []
        buff = b"*%d\r\n" % len(args)

        for k in map(self.encoder.encode, args):
            if len(buff) > 6000 or len(k) > 6000:
                output.append(b"%s$%d\r\n" % (buff, len(k)))
                output.append(k)
                buff = SYM_CRLF
            else:
                buff = b"%s$%d\r\n%s\r\n" % (buff, len(k), k)
        output.append(buff)
        return output


class CommandPackerBenchmark(Benchmark):
    ARGUMENTS = (
        {
            "name": "connection_class",
            "values": [StringJoiningConnection, ListJoiningConnection],
        },
        {
            "name": "value_size",
            "values": [10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000],
        },
    )

    def setup(self, connection_class, value_size):
        self.get_client(connection_class=connection_class)

    def run(self, connection_class, value_size):
        r = self.get_client()
        x = "a" * value_size
        r.set("benchmark", x)


if __name__ == "__main__":
    CommandPackerBenchmark().run_benchmark()
