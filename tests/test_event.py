from unittest.mock import Mock, AsyncMock

import pytest

from redis.credentials import StreamingCredentialProvider
from redis.event import (
    AfterPooledConnectionsInstantiationEvent,
    ClientType,
    EventListenerInterface,
    EventDispatcher,
    AsyncEventListenerInterface,
    RegisterReAuthForPooledConnections,
)
from redis.exceptions import ConnectionError


@pytest.mark.fixed_client
class TestEventDispatcher:
    def test_register_listeners(self):
        mock_event = Mock(spec=object)
        mock_event_listener = Mock(spec=EventListenerInterface)
        listener_called = 0

        def callback(event):
            nonlocal listener_called
            listener_called += 1

        mock_event_listener.listen = callback

        # Register via constructor
        dispatcher = EventDispatcher(
            event_listeners={type(mock_event): [mock_event_listener]}
        )
        dispatcher.dispatch(mock_event)

        assert listener_called == 1

        # Register additional listener for the same event
        mock_another_event_listener = Mock(spec=EventListenerInterface)
        mock_another_event_listener.listen = callback
        dispatcher.register_listeners(
            mappings={type(mock_event): [mock_another_event_listener]}
        )
        dispatcher.dispatch(mock_event)

        assert listener_called == 3

    async def test_register_listeners_async(self):
        mock_event = Mock(spec=object)
        mock_event_listener = AsyncMock(spec=AsyncEventListenerInterface)
        listener_called = 0

        async def callback(event):
            nonlocal listener_called
            listener_called += 1

        mock_event_listener.listen = callback

        # Register via constructor
        dispatcher = EventDispatcher(
            event_listeners={type(mock_event): [mock_event_listener]}
        )
        await dispatcher.dispatch_async(mock_event)

        assert listener_called == 1

        # Register additional listener for the same event
        mock_another_event_listener = Mock(spec=AsyncEventListenerInterface)
        mock_another_event_listener.listen = callback
        dispatcher.register_listeners(
            mappings={type(mock_event): [mock_another_event_listener]}
        )
        await dispatcher.dispatch_async(mock_event)

        assert listener_called == 3

    def test_listener_can_unregister_itself_during_dispatch(self):
        """Listener calling unregister_listeners from inside listen() must
        not deadlock (dispatch must not hold the lock across invocation)."""

        class _Evt:
            pass

        dispatcher = EventDispatcher()

        class SelfUnregisteringListener(EventListenerInterface):
            def __init__(self) -> None:
                self.calls = 0

            def listen(self, event: object) -> None:
                self.calls += 1
                dispatcher.unregister_listeners({type(event): [self]})

        listener = SelfUnregisteringListener()
        dispatcher.register_listeners({_Evt: [listener]})

        dispatcher.dispatch(_Evt())
        dispatcher.dispatch(_Evt())

        assert listener.calls == 1

    def test_listener_can_register_during_dispatch(self):
        """Listener calling register_listeners from inside listen() must
        not deadlock, and the newly registered listener must not receive
        the in-flight event (snapshot semantics)."""

        class _Evt:
            pass

        dispatcher = EventDispatcher()
        later_calls = 0

        class LateListener(EventListenerInterface):
            def listen(self, event: object) -> None:
                nonlocal later_calls
                later_calls += 1

        late = LateListener()

        class RegisteringListener(EventListenerInterface):
            def listen(self, event: object) -> None:
                dispatcher.register_listeners({type(event): [late]})

        dispatcher.register_listeners({_Evt: [RegisteringListener()]})

        dispatcher.dispatch(_Evt())
        assert later_calls == 0
        dispatcher.dispatch(_Evt())
        assert later_calls == 1

    async def test_listener_can_unregister_itself_during_dispatch_async(self):
        class _Evt:
            pass

        dispatcher = EventDispatcher()

        class SelfUnregisteringListener(AsyncEventListenerInterface):
            def __init__(self) -> None:
                self.calls = 0

            async def listen(self, event: object) -> None:
                self.calls += 1
                dispatcher.unregister_listeners({type(event): [self]})

        listener = SelfUnregisteringListener()
        dispatcher.register_listeners({_Evt: [listener]})

        await dispatcher.dispatch_async(_Evt())
        await dispatcher.dispatch_async(_Evt())

        assert listener.calls == 1

    async def test_listener_can_register_during_dispatch_async(self):
        class _Evt:
            pass

        dispatcher = EventDispatcher()
        later_calls = 0

        class LateListener(AsyncEventListenerInterface):
            async def listen(self, event: object) -> None:
                nonlocal later_calls
                later_calls += 1

        late = LateListener()

        class RegisteringListener(AsyncEventListenerInterface):
            async def listen(self, event: object) -> None:
                dispatcher.register_listeners({type(event): [late]})

        dispatcher.register_listeners({_Evt: [RegisteringListener()]})

        await dispatcher.dispatch_async(_Evt())
        assert later_calls == 0
        await dispatcher.dispatch_async(_Evt())
        assert later_calls == 1


@pytest.mark.fixed_client
class TestRegisterReAuthForPooledConnections:
    async def test_failed_re_auth_does_not_stop_the_other_pools_async(self):
        credential_provider = Mock(spec=StreamingCredentialProvider)
        pools = [Mock(), Mock(), Mock()]
        pools[0].re_auth_callback = AsyncMock(side_effect=ConnectionError("first"))
        pools[1].re_auth_callback = AsyncMock(side_effect=ConnectionError("second"))
        pools[2].re_auth_callback = AsyncMock()
        token = Mock()

        RegisterReAuthForPooledConnections().listen(
            AfterPooledConnectionsInstantiationEvent(
                pools, ClientType.ASYNC, credential_provider
            )
        )
        re_auth = credential_provider.on_next.call_args.args[0]

        with pytest.raises(ConnectionError, match="first"):
            await re_auth(token)

        for pool in pools:
            pool.re_auth_callback.assert_awaited_once_with(token)
