Lock
#########

Expiration and ownership
========================

A lock's ``timeout`` limits its lifetime in Redis. It does not stop the work
running inside the critical section. If that work outlives the timeout, another
client can acquire the lock while the original client is still working.

``release()`` raises :class:`redis.exceptions.LockNotOwnedError` when the lock's
key is missing or contains another client's token. A missing key does not mean
the critical section remained exclusive: after the original lease expired,
another client could have acquired and released the lock before the original
client tried to release it. Both cases mean the original client lost ownership.

By default, this error also propagates when leaving a lock's context manager.
To log a warning instead, use ``raise_on_release_error=False``:

.. code-block:: python

    import redis

    r = redis.Redis()
    with r.lock("resource-lock", timeout=30, raise_on_release_error=False):
        # Work that is safe to finish even if the lease expires.
        pass

This option affects context-manager cleanup only. An explicit ``release()``
still raises when ownership has been lost. The same option is available on
``redis.asyncio.Redis.lock()`` with ``async with``.

Suppressing the cleanup error does not restore mutual exclusion. ``owned()``
checks ownership at the instant it runs; ownership can change afterward.
``extend()`` can renew a lease that is still owned, and raises
``LockNotOwnedError`` if ownership has already been lost. Neither operation
makes a separate database transaction atomic with the Redis lock. If a resource
must reject writes from an expired owner, enforce that at the resource, for
example with fencing tokens.

.. automodule:: redis.lock
    :members: 
