Retry Helpers
#############

.. automodule:: redis.retry
    :members:


Retry in Redis Standalone
**************************

>>> from redis.backoff import ExponentialBackoff
>>> from redis.retry import Retry
>>> from redis.client import Redis
>>> from redis.exceptions import (
>>>    BusyLoadingError,
>>>    RedisError,
>>> )
>>>
>>> # Run 3 retries with exponential backoff strategy
>>> retry = Retry(ExponentialBackoff(), 3)
>>> # Redis client with retries on custom errors in addition to the errors
>>> # that are already retried by default
>>> r = Redis(host='localhost', port=6379, retry=retry, retry_on_error=[BusyLoadingError, RedisError])

As you can see from the example above, Redis client supports 2 parameters to configure the retry behaviour:

* ``retry``: :class:`~.Retry` instance with a :ref:`backoff-label` strategy and the max number of retries
    * The :class:`~.Retry` instance has default set of :ref:`exceptions-label` to retry on,
      which can be overridden by passing a tuple with :ref:`exceptions-label` to the ``supported_errors`` parameter.
* ``retry_on_error``: list of additional :ref:`exceptions-label` to retry on

``retry_on_error`` can also be set in a connection URL as a comma-separated
list of names from :mod:`redis.exceptions` (for example
``redis://localhost?retry_on_error=ConnectionError,TimeoutError``).


If no ``retry`` is provided, a default one is created with  :class:`~.ExponentialWithJitterBackoff` as backoff strategy
and 3 retries.


Retry with asyncio
******************

Async clients use ``redis.asyncio.retry.Retry`` so retry callbacks and backoff
delays can be awaited:

>>> from redis.asyncio import Redis
>>> from redis.asyncio.retry import Retry
>>> from redis.backoff import ExponentialBackoff
>>>
>>> retry = Retry(ExponentialBackoff(), 3)
>>> r = Redis(host='localhost', port=6379, retry=retry)

Passing :class:`redis.retry.Retry` to an async client is supported for
compatibility, but it is converted to ``redis.asyncio.retry.Retry`` and emits a
warning. Custom synchronous ``call_with_retry`` implementations cannot be
preserved by that conversion, so async clients should be configured with an
async retry policy directly.


Retry in Redis Cluster
**************************

>>> from redis.backoff import ExponentialBackoff
>>> from redis.retry import Retry
>>> from redis.cluster import RedisCluster
>>>
>>> # Run 3 retries with exponential backoff strategy
>>> retry = Retry(ExponentialBackoff(), 3)
>>> # Redis Cluster client with retries
>>> rc = RedisCluster(host='localhost', port=6379, retry=retry)

Retry behaviour in Redis Cluster is a little bit different from Standalone:

* ``retry``: :class:`~.Retry` instance with a :ref:`backoff-label` strategy and the max number of retries, default value is ``Retry(ExponentialWithJitterBackoff(base=0.01, cap=1), cluster_error_retry_attempts)``
* ``cluster_error_retry_attempts``: number of times to retry before raising an error when :class:`~.TimeoutError`, :class:`~.ConnectionError`, :class:`~.ClusterDownError` or :class:`~.SlotNotCoveredError` are encountered, default value is ``10``
    * This argument is deprecated - it is used to initialize the number of retries for the retry object,
      only in the case when the ``retry`` object is not provided.
      When the ``retry`` argument is provided, the ``cluster_error_retry_attempts`` argument is ignored!

* The retry object sets both the number of retries and the delay before each one:
  the cluster client waits as long as its backoff says before it retries a command or a pipeline.
  See :ref:`cluster-retries-label` for the delays and for how often the topology is refreshed.

Let's consider the following example:

>>> from redis.backoff import ExponentialBackoff
>>> from redis.retry import Retry
>>> from redis.cluster import RedisCluster
>>>
>>> rc = RedisCluster(host='localhost', port=6379, retry=Retry(ExponentialBackoff(), 6))
>>> rc.set('foo', 'bar')

#. the client library calculates the hash slot for key 'foo'.
#. given the hash slot, it then determines which node to connect to, in order to execute the command.
#. during the connection, a :class:`~.ConnectionError` is raised.
#. because we set ``retry=Retry(ExponentialBackoff(), 6)``, the cluster client asks for
   a refresh of the cluster topology, which tries the failed node last. The request is
   shared by the whole client: the first command attempt that starts once the refresh
   is due runs it, whether that is the retry of this command or another command.
   Refreshes run at most once per second, so a retry inside that second still uses the
   current topology.
#. the cluster client waits as long as the backoff says and retries the command, until it either succeeds or the max number of retries is reached.
