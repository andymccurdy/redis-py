from logging import getLogger
from typing import Any, Union

from ..exceptions import ConnectionError, InvalidResponse, ResponseError
from ..typing import EncodableT
from ..utils import SENTINEL
from .base import (
    AsyncPushNotificationsParser,
    PushNotificationsParser,
    _AsyncRESPBase,
    _parse_float,
    _parse_int,
    _RESPBase,
)
from .socket import SERVER_CLOSED_CONNECTION_ERROR


class _RESP3Parser(_RESPBase, PushNotificationsParser):
    """RESP3 protocol implementation"""

    def __init__(self, socket_read_size):
        super().__init__(socket_read_size)
        self.pubsub_push_handler_func = self.handle_pubsub_push_response
        self.node_moving_push_handler_func = None
        self.maintenance_push_handler_func = None
        self.oss_cluster_maint_push_handler_func = None
        self.invalidation_push_handler_func = None

    def handle_pubsub_push_response(self, response):
        logger = getLogger("push_response")
        logger.debug("Push response: %s", response)
        return response

    def read_response(
        self,
        disable_decoding=False,
        push_request=False,
        timeout: Union[float, object] = SENTINEL,
    ):
        pos = self._buffer.get_pos() if self._buffer is not None else None
        try:
            result = self._read_response(
                disable_decoding=disable_decoding,
                push_request=push_request,
                timeout=timeout,
            )
        except BaseException:
            if self._buffer is not None:
                self._buffer.rewind(pos)
            raise
        else:
            if self._buffer is not None:
                try:
                    self._buffer.purge()
                except AttributeError:
                    # Buffer may have been set to None by another thread after
                    # the check above; result is still valid so we don't raise
                    pass
            return result

    def _read_response(
        self,
        disable_decoding=False,
        push_request=False,
        timeout: Union[float, object] = SENTINEL,
    ):
        raw = self._buffer.readline(timeout=timeout)
        if not raw:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)

        byte, response = raw[:1], raw[1:]

        # server returned an error
        if byte in (b"-", b"!"):
            if byte == b"!":
                response = self._buffer.read(_parse_int(response, raw), timeout=timeout)
            response = response.decode("utf-8", errors="replace")
            error = self.parse_error(response)
            # if the error is a ConnectionError, raise immediately so the user
            # is notified
            if isinstance(error, ConnectionError):
                raise error
            # otherwise, we're dealing with a ResponseError that might belong
            # inside a pipeline response. the connection's read_response()
            # and/or the pipeline's execute() will raise this error if
            # necessary, so just return the exception instance here.
            return error
        # single value
        elif byte == b"+":
            pass
        # null value
        elif byte == b"_":
            return None
        # int and big int values
        elif byte in (b":", b"("):
            return _parse_int(response, raw)
        # double value
        elif byte == b",":
            return _parse_float(response, raw)
        # bool value
        elif byte == b"#":
            return response == b"t"
        # bulk response
        elif byte == b"$":
            response = self._buffer.read(_parse_int(response, raw), timeout=timeout)
        # verbatim string response
        elif byte == b"=":
            response = self._buffer.read(_parse_int(response, raw), timeout=timeout)[4:]
        # array response
        elif byte == b"*":
            response = [
                self._read_response(disable_decoding=disable_decoding, timeout=timeout)
                for _ in range(_parse_int(response, raw))
            ]
        # set response
        elif byte == b"~":
            # redis can return unhashable types (like dict) in a set,
            # so we return sets as list, all the time, for predictability
            response = [
                self._read_response(disable_decoding=disable_decoding, timeout=timeout)
                for _ in range(_parse_int(response, raw))
            ]
        # map response
        elif byte == b"%":
            # We cannot use a dict-comprehension to parse stream.
            # Evaluation order of key:val expression in dict comprehension only
            # became defined to be left-right in version 3.8
            resp_dict = {}
            for _ in range(_parse_int(response, raw)):
                key = self._read_response(
                    disable_decoding=disable_decoding, timeout=timeout
                )
                resp_dict[key] = self._read_response(
                    disable_decoding=disable_decoding,
                    push_request=push_request,
                    timeout=timeout,
                )
            response = resp_dict
        # attribute response: auxiliary data that comes before the reply it
        # describes. skip it and read the reply
        elif byte == b"|":
            for _ in range(_parse_int(response, raw) * 2):
                self._skip_response(timeout=timeout)
            return self._read_response(
                disable_decoding=disable_decoding,
                push_request=push_request,
                timeout=timeout,
            )
        # push response
        elif byte == b">":
            response = [
                self._read_response(
                    disable_decoding=disable_decoding,
                    push_request=push_request,
                    timeout=timeout,
                )
                for _ in range(_parse_int(response, raw))
            ]
            response = self.handle_push_response(response)

            # if this is a push request return the push response
            if push_request:
                return response

            # `timeout` has to be forwarded here too: it is the caller's bound
            # for this read, and dropping it would silently fall back to the
            # connection's socket_timeout for the rest of the response just
            # because a push message happened to interleave. The hiredis parser
            # already forwards it on the same continuation.
            return self._read_response(
                disable_decoding=disable_decoding,
                push_request=push_request,
                timeout=timeout,
            )
        else:
            raise InvalidResponse(f"Protocol Error: {raw!r}")

        if isinstance(response, bytes) and disable_decoding is False:
            response = self.encoder.decode(response)

        return response

    def _skip_response(self, timeout: Union[float, object] = SENTINEL) -> None:
        # read a part of an attribute without handling it as a reply: an error
        # in it isn't raised, and nothing in it is decoded
        raw = self._buffer.readline(timeout=timeout)
        if not raw:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)

        byte, response = raw[:1], raw[1:]

        if byte in (b"$", b"=", b"!"):
            self._buffer.read(_parse_int(response, raw), timeout=timeout)
        elif byte in (b"*", b"~", b">"):
            for _ in range(_parse_int(response, raw)):
                self._skip_response(timeout=timeout)
        elif byte == b"%":
            for _ in range(_parse_int(response, raw) * 2):
                self._skip_response(timeout=timeout)
        # an attribute, and the value it describes
        elif byte == b"|":
            for _ in range(_parse_int(response, raw) * 2 + 1):
                self._skip_response(timeout=timeout)
        elif byte not in (b"-", b"+", b"_", b":", b"(", b",", b"#"):
            raise InvalidResponse(f"Protocol Error: {raw!r}")


class _AsyncRESP3Parser(_AsyncRESPBase, AsyncPushNotificationsParser):
    def __init__(self, socket_read_size):
        super().__init__(socket_read_size)
        self.pubsub_push_handler_func = self.handle_pubsub_push_response
        self.invalidation_push_handler_func = None

    async def handle_pubsub_push_response(self, response):
        logger = getLogger("push_response")
        logger.debug("Push response: %s", response)
        return response

    async def read_response(
        self, disable_decoding: bool = False, push_request: bool = False
    ):
        if not self._connected:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)
        if self._chunks:
            # augment parsing buffer with previously read data
            self._buffer += b"".join(self._chunks)
            self._chunks.clear()
        self._pos = 0
        response = await self._read_response(
            disable_decoding=disable_decoding, push_request=push_request
        )
        # Successfully parsing a response allows us to clear our parsing buffer
        self._clear()
        return response

    async def _read_response(
        self, disable_decoding: bool = False, push_request: bool = False
    ) -> Union[EncodableT, ResponseError, None]:
        if not self._stream or not self.encoder:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)
        raw = await self._readline()
        response: Any
        byte, response = raw[:1], raw[1:]

        # if byte not in (b"-", b"+", b":", b"$", b"*"):
        #     raise InvalidResponse(f"Protocol Error: {raw!r}")

        # server returned an error
        if byte in (b"-", b"!"):
            if byte == b"!":
                response = await self._read(_parse_int(response, raw))
            response = response.decode("utf-8", errors="replace")
            error = self.parse_error(response)
            # if the error is a ConnectionError, raise immediately so the user
            # is notified
            if isinstance(error, ConnectionError):
                self._clear()  # Successful parse
                raise error
            # otherwise, we're dealing with a ResponseError that might belong
            # inside a pipeline response. the connection's read_response()
            # and/or the pipeline's execute() will raise this error if
            # necessary, so just return the exception instance here.
            return error
        # single value
        elif byte == b"+":
            pass
        # null value
        elif byte == b"_":
            return None
        # int and big int values
        elif byte in (b":", b"("):
            return _parse_int(response, raw)
        # double value
        elif byte == b",":
            return _parse_float(response, raw)
        # bool value
        elif byte == b"#":
            return response == b"t"
        # bulk response
        elif byte == b"$":
            response = await self._read(_parse_int(response, raw))
        # verbatim string response
        elif byte == b"=":
            response = (await self._read(_parse_int(response, raw)))[4:]
        # array response
        elif byte == b"*":
            response = [
                (await self._read_response(disable_decoding=disable_decoding))
                for _ in range(_parse_int(response, raw))
            ]
        # set response
        elif byte == b"~":
            # redis can return unhashable types (like dict) in a set,
            # so we always convert to a list, to have predictable return types
            response = [
                (await self._read_response(disable_decoding=disable_decoding))
                for _ in range(_parse_int(response, raw))
            ]
        # map response
        elif byte == b"%":
            # We cannot use a dict-comprehension to parse stream.
            # Evaluation order of key:val expression in dict comprehension only
            # became defined to be left-right in version 3.8
            resp_dict = {}
            for _ in range(_parse_int(response, raw)):
                key = await self._read_response(disable_decoding=disable_decoding)
                resp_dict[key] = await self._read_response(
                    disable_decoding=disable_decoding, push_request=push_request
                )
            response = resp_dict
        # attribute response: auxiliary data that comes before the reply it
        # describes. skip it and read the reply
        elif byte == b"|":
            for _ in range(_parse_int(response, raw) * 2):
                await self._skip_response()
            return await self._read_response(
                disable_decoding=disable_decoding, push_request=push_request
            )
        # push response
        elif byte == b">":
            response = [
                (
                    await self._read_response(
                        disable_decoding=disable_decoding, push_request=push_request
                    )
                )
                for _ in range(_parse_int(response, raw))
            ]
            response = await self.handle_push_response(response)
            if not push_request:
                return await self._read_response(
                    disable_decoding=disable_decoding, push_request=push_request
                )
            else:
                return response
        else:
            raise InvalidResponse(f"Protocol Error: {raw!r}")

        if isinstance(response, bytes) and disable_decoding is False:
            response = self.encoder.decode(response)
        return response

    async def _skip_response(self) -> None:
        # read a part of an attribute without handling it as a reply: an error
        # in it isn't raised, and nothing in it is decoded
        raw = await self._readline()
        byte, response = raw[:1], raw[1:]

        if byte in (b"$", b"=", b"!"):
            await self._read(_parse_int(response, raw))
        elif byte in (b"*", b"~", b">"):
            for _ in range(_parse_int(response, raw)):
                await self._skip_response()
        elif byte == b"%":
            for _ in range(_parse_int(response, raw) * 2):
                await self._skip_response()
        # an attribute, and the value it describes
        elif byte == b"|":
            for _ in range(_parse_int(response, raw) * 2 + 1):
                await self._skip_response()
        elif byte not in (b"-", b"+", b"_", b":", b"(", b",", b"#"):
            raise InvalidResponse(f"Protocol Error: {raw!r}")
