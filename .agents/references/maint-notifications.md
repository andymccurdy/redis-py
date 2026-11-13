# Maintenance notifications

Read this before you change `redis/maint_notifications.py`, the push-notification handling in the connection mixins, or the MultiDB health check timeouts.

`redis/maint_notifications.py` holds the server-pushed maintenance notifications and the handlers that react to them.

## Events

The handlers report the relaxation they apply as `MaintenanceStartedEvent` / `MaintenanceCompletedEvent` (`redis/event.py`) to the optional `event_dispatcher` on `MaintNotificationsConfig`.

The in-tree consumer is the MultiDB health check policy (`redis/asyncio/multidb/healthcheck.py`). While its probe client is under maintenance, it relaxes a database's `health_check_timeout` to `probes * (relaxed_timeout + delay)`.

## Idle connections with buffered pushes

With notifications enabled, `ensure_connection` does not reconnect a pooled connection outright when it has readable data while idle. Instead, `handle_pending_push_notifications` (in both connection mixins) does this:

1. It drains the buffered pushes with the handlers deferred.
2. If the socket is still alive, the maintenance is still in progress, so it applies them.
3. If the server has closed the socket or a stale reply was left on it, it discards them and raises, so the pool reconnects.
