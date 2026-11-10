Advanced Features
=================

A note about threading
----------------------

Redis client instances can safely be shared between threads. Internally,
connection instances are only retrieved from the connection pool during
command execution, and returned to the pool directly after. Command
execution never modifies state on the client instance.

However, there is one caveat: the Redis SELECT command. The SELECT
command allows you to switch the database currently in use by the
connection. That database remains selected until another is selected or
until the connection is closed. This creates an issue in that
connections could be returned to the pool that are connected to a
different database.

As a result, redis-py does not implement the SELECT command on client
instances. If you use multiple Redis databases within the same
application, you should create a separate client instance (and possibly
a separate connection pool) for each database.

It is not safe to pass PubSub or Pipeline objects between threads.

Pipelines
---------

Default pipelines
~~~~~~~~~~~~~~~~~

Pipelines are a subclass of the base Redis class that provide support
for buffering multiple commands to the server in a single request. They
can be used to dramatically increase the performance of groups of
commands by reducing the number of back-and-forth TCP packets between
the client and server.

Pipelines are quite simple to use:

.. code:: python

   >>> r = redis.Redis(...)
   >>> r.set('bing', 'baz')
   >>> # Use the pipeline() method to create a pipeline instance
   >>> pipe = r.pipeline()
   >>> # The following SET commands are buffered
   >>> pipe.set('foo', 'bar')
   >>> pipe.get('bing')
   >>> # the EXECUTE call sends all buffered commands to the server, returning
   >>> # a list of responses, one for each command.
   >>> pipe.execute()
   [True, b'baz']

For ease of use, all commands being buffered into the pipeline return
the pipeline object itself. Therefore calls can be chained like:

.. code:: python

   >>> pipe.set('foo', 'bar').sadd('faz', 'baz').incr('auto_number').execute()
   [True, True, 6]

In addition, pipelines can also ensure the buffered commands are
executed atomically as a group. This happens by default. If you want to
disable the atomic nature of a pipeline but still want to buffer
commands, you can turn off transactions.

.. code:: python

   >>> pipe = r.pipeline(transaction=False)

A common issue occurs when requiring atomic transactions but needing to
retrieve values in Redis prior for use within the transaction. For
instance, let's assume that the INCR command didn't exist and we need to
build an atomic version of INCR in Python.

The completely naive implementation could GET the value, increment it in
Python, and SET the new value back. However, this is not atomic because
multiple clients could be doing this at the same time, each getting the
same value from GET.

Enter the WATCH command. WATCH provides the ability to monitor one or
more keys prior to starting a transaction. If any of those keys change
prior the execution of that transaction, the entire transaction will be
canceled and a WatchError will be raised. To implement our own
client-side INCR command, we could do something like this:

.. code:: python

   >>> with r.pipeline() as pipe:
   ...     while True:
   ...         try:
   ...             # put a WATCH on the key that holds our sequence value
   ...             pipe.watch('OUR-SEQUENCE-KEY')
   ...             # after WATCHing, the pipeline is put into immediate execution
   ...             # mode until we tell it to start buffering commands again.
   ...             # this allows us to get the current value of our sequence
   ...             current_value = pipe.get('OUR-SEQUENCE-KEY')
   ...             next_value = int(current_value) + 1
   ...             # now we can put the pipeline back into buffered mode with MULTI
   ...             pipe.multi()
   ...             pipe.set('OUR-SEQUENCE-KEY', next_value)
   ...             # and finally, execute the pipeline (the set command)
   ...             pipe.execute()
   ...             # if a WatchError wasn't raised during execution, everything
   ...             # we just did happened atomically.
   ...             break
   ...        except WatchError:
   ...             # another client must have changed 'OUR-SEQUENCE-KEY' between
   ...             # the time we started WATCHing it and the pipeline's execution.
   ...             # our best bet is to just retry.
   ...             continue

Note that, because the Pipeline must bind to a single connection for the
duration of a WATCH, care must be taken to ensure that the connection is
returned to the connection pool by calling the reset() method. If the
Pipeline is used as a context manager (as in the example above) reset()
will be called automatically. Of course you can do this the manual way
by explicitly calling reset():

.. code:: python

   >>> pipe = r.pipeline()
   >>> while True:
   ...     try:
   ...         pipe.watch('OUR-SEQUENCE-KEY')
   ...         ...
   ...         pipe.execute()
   ...         break
   ...     except WatchError:
   ...         continue
   ...     finally:
   ...         pipe.reset()

A convenience method named "transaction" exists for handling all the
boilerplate of handling and retrying watch errors. It takes a callable
that should expect a single parameter, a pipeline object, and any number
of keys to be WATCHed. Our client-side INCR command above can be written
like this, which is much easier to read:

.. code:: python

   >>> def client_side_incr(pipe):
   ...     current_value = pipe.get('OUR-SEQUENCE-KEY')
   ...     next_value = int(current_value) + 1
   ...     pipe.multi()
   ...     pipe.set('OUR-SEQUENCE-KEY', next_value)
   >>>
   >>> r.transaction(client_side_incr, 'OUR-SEQUENCE-KEY')
   [True]

Be sure to call pipe.multi() in the callable passed to Redis.transaction
prior to any write commands.

Pipelines in clusters
~~~~~~~~~~~~~~~~~~~~~

ClusterPipeline is a subclass of RedisCluster that provides support for
Redis pipelines in cluster mode. When calling the execute() command, all
the commands are grouped by the node on which they will be executed, and
are then executed by the respective nodes in parallel. The pipeline
instance will wait for all the nodes to respond before returning the
result to the caller. Command responses are returned as a list sorted in
the same order in which they were sent. Pipelines can be used to
dramatically increase the throughput of Redis Cluster by significantly
reducing the number of network round trips between the client and
the server.

.. code:: python

   >>> rc = RedisCluster()
   >>> with rc.pipeline() as pipe:
   ...     pipe.set('foo', 'value1')
   ...     pipe.set('bar', 'value2')
   ...     pipe.get('foo')
   ...     pipe.get('bar')
   ...     print(pipe.execute())
   [True, True, b'value1', b'value2']
   ...     pipe.set('foo1', 'bar1').get('foo1').execute()
   [True, b'bar1']

Please note:

-  RedisCluster pipelines currently only support key-based commands.
-  The pipeline gets its ‘load_balancing_strategy’ value from the
   cluster’s parameter. Thus, if read from replications is enabled in
   the cluster instance, the pipeline will also direct read commands to
   replicas.


Transactions in clusters
~~~~~~~~~~~~~~~~~~~~~~~~

Transactions are supported in cluster-mode with one caveat: all keys of
all commands issued on a transaction pipeline must reside on the
same slot. This is similar to the limitation of multikey commands in
cluster. The reason behind this is that the Redis engine does not offer
a mechanism to block or exchange key data across nodes on the fly. A
client may add some logic to abstract engine limitations when running
on a cluster, such as the pipeline behavior explained on the previous
block, but there is no simple way that a client can enforce atomicity
across nodes on a distributed system.

The compromise of limiting the transaction pipeline to same-slot keys
is exactly that: a compromise. While this behavior is different from
non-transactional cluster pipelines, it simplifies migration of clients
from standalone to cluster under some circumstances. Note that application
code that issues multi/exec commands on a standalone client without
embedding them within a pipeline would eventually get ‘AttributeError’.
With this approach, if the application uses ‘client.pipeline(transaction=True)’,
then switching the client with a cluster-aware instance would simplify
code changes (to some extent). This may be true for application code that
makes use of hash keys, since its transactions may already be
mapping all commands to the same slot.

An alternative is some kind of two-step commit solution, where a slot
validation is run before the actual commands are run. This could work
with controlled node maintenance but does not cover single node failures.

Given the cluster limitations for transactions, by default pipeline isn't in
transactional mode. To enable transactional context set:

.. code:: python

   >>> p = rc.pipeline(transaction=True)

After entering the transactional context you can add commands to a transactional
context, by one of the following ways:

.. code:: python

   >>> p = rc.pipeline(transaction=True) # Chaining commands
   >>> p.set("key", "value")
   >>> p.get("key")
   >>> response = p.execute()

Or

.. code:: python

   >>> with rc.pipeline(transaction=True) as pipe: # Using context manager
   ...     pipe.set("key", "value")
   ...     pipe.get("key")
   ...     response = pipe.execute()

As you see there's no need to explicitly send `MULTI/EXEC` commands to control context start/end
`ClusterPipeline` will take care of it.

To ensure that different keys will be mapped to a same hash slot on the server side
prepend your keys with the same hash tag, the technique that allows you to control
keys distribution.
More information `here <https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/#hash-tags>`_

.. code:: python

   >>> with rc.pipeline(transaction=True) as pipe:
   ...     pipe.set("{tag}foo", "bar")
   ...     pipe.set("{tag}bar", "foo")
   ...     pipe.get("{tag}foo")
   ...     pipe.get("{tag}bar")
   ...     response = pipe.execute()

CAS Transactions
~~~~~~~~~~~~~~~~~~~~~~~~

If you want to apply optimistic locking for certain keys, you have to execute
`WATCH` command in transactional context. `WATCH` command follows the same limitations
as any other multi key command - all keys should be mapped to the same hash slot.

However, the difference between CAS transaction and normal one is that you have to
explicitly call MULTI command to indicate the start of transactional context, WATCH
command itself and any subsequent commands before MULTI will be immediately executed
on the server side so you can apply optimistic locking and get necessary data before
transaction execution.

.. code:: python

   >>> with rc.pipeline(transaction=True) as pipe:
   ...     pipe.watch("mykey")       # Apply locking by immediately executing command
   ...     val = pipe.get("mykey")   # Immediately retrieves value
   ...     val = val + 1             # Increment value
   ...     pipe.multi()              # Starting transaction context
   ...     pipe.set("mykey", val)    # Command will be pipelined
   ...     response = pipe.execute() # Returns OK or None if key was modified in the meantime


Publish / Subscribe
-------------------

redis-py includes a PubSub object that subscribes to channels and
listens for new messages. Creating a PubSub object is easy.

.. code:: python

   >>> r = redis.Redis(...)
   >>> p = r.pubsub()

Once a PubSub instance is created, channels and patterns can be
subscribed to.

.. code:: python

   >>> p.subscribe('my-first-channel', 'my-second-channel', ...)
   >>> p.psubscribe('my-*', ...)

The PubSub instance is now subscribed to those channels/patterns. The
subscription confirmations can be seen by reading messages from the
PubSub instance.

.. code:: python

   >>> p.get_message()
   {'pattern': None, 'type': 'subscribe', 'channel': b'my-second-channel', 'data': 1}
   >>> p.get_message()
   {'pattern': None, 'type': 'subscribe', 'channel': b'my-first-channel', 'data': 2}
   >>> p.get_message()
   {'pattern': None, 'type': 'psubscribe', 'channel': b'my-*', 'data': 3}

Every message read from a PubSub instance will be a dictionary with the
following keys.

-  **type**: One of the following: 'subscribe', 'unsubscribe',
   'psubscribe', 'punsubscribe', 'message', 'pmessage'
-  **channel**: The channel [un]subscribed to or the channel a message
   was published to
-  **pattern**: The pattern that matched a published message's channel.
   Will be None in all cases except for 'pmessage' types.
-  **data**: The message data. With [un]subscribe messages, this value
   will be the number of channels and patterns the connection is
   currently subscribed to. With [p]message messages, this value will be
   the actual published message.

Let's send a message now.

.. code:: python

   # the publish method returns the number matching channel and pattern
   # subscriptions. 'my-first-channel' matches both the 'my-first-channel'
   # subscription and the 'my-*' pattern subscription, so this message will
   # be delivered to 2 channels/patterns
   >>> r.publish('my-first-channel', 'some data')
   2
   >>> p.get_message()
   {'channel': b'my-first-channel', 'data': b'some data', 'pattern': None, 'type': 'message'}
   >>> p.get_message()
   {'channel': b'my-first-channel', 'data': b'some data', 'pattern': b'my-*', 'type': 'pmessage'}

Unsubscribing works just like subscribing. If no arguments are passed to
[p]unsubscribe, all channels or patterns will be unsubscribed from.

.. code:: python

   >>> p.unsubscribe()
   >>> p.punsubscribe('my-*')
   >>> p.get_message()
   {'channel': b'my-second-channel', 'data': 2, 'pattern': None, 'type': 'unsubscribe'}
   >>> p.get_message()
   {'channel': b'my-first-channel', 'data': 1, 'pattern': None, 'type': 'unsubscribe'}
   >>> p.get_message()
   {'channel': b'my-*', 'data': 0, 'pattern': None, 'type': 'punsubscribe'}

redis-py also allows you to register callback functions to handle
published messages. Message handlers take a single argument, the
message, which is a dictionary just like the examples above. To
subscribe to a channel or pattern with a message handler, pass the
channel or pattern name as a keyword argument with its value being the
callback function. For channel or pattern names that cannot be used as
Python keyword arguments, including arbitrary binary names, pass a
``redis.Subscription`` object as a positional argument instead.

Subscription forms can be mixed in the same call. Assuming
``my_handler`` is a callback, a plain channel name can be supplied
alongside a ``Subscription`` object, and ``Subscription`` objects can
also be used together with the existing keyword-argument handler style.

.. code:: python

   >>> p.subscribe('logs', redis.Subscription(b'\x80\x81', my_handler))
   >>> p.subscribe(redis.Subscription(b'\x82\x83', my_handler), **{'my-channel': my_handler})

When a message is read on a channel or pattern with a message handler,
the message dictionary is created and passed to the message handler. In
this case, a None value is returned from get_message() since the message
was already handled.

.. code:: python

   >>> def my_handler(message):
   ...     print('MY HANDLER: ', message['data'])
   >>> p.subscribe(**{'my-channel': my_handler})
   >>> p.subscribe(redis.Subscription(b'\x80\x81', my_handler))
   # read the subscribe confirmation message
   >>> p.get_message()
   {'pattern': None, 'type': 'subscribe', 'channel': b'my-channel', 'data': 1}
   >>> r.publish('my-channel', 'awesome data')
   1
   # for the message handler to work, we need tell the instance to read data.
   # this can be done in several ways (read more below). we'll just use
   # the familiar get_message() function for now
   >>> message = p.get_message()
   MY HANDLER:  awesome data
   # note here that the my_handler callback printed the string above.
   # `message` is None because the message was handled by our handler.
   >>> print(message)
   None

If your application is not interested in the (sometimes noisy)
subscribe/unsubscribe confirmation messages, you can ignore them by
passing ignore_subscribe_messages=True to r.pubsub(). This will cause
all subscribe/unsubscribe messages to be read, but they won't bubble up
to your application.

.. code:: python

   >>> p = r.pubsub(ignore_subscribe_messages=True)
   >>> p.subscribe('my-channel')
   >>> p.get_message()  # hides the subscribe message and returns None
   >>> r.publish('my-channel', 'my data')
   1
   >>> p.get_message()
   {'channel': b'my-channel', 'data': b'my data', 'pattern': None, 'type': 'message'}

There are three different strategies for reading messages.

The examples above have been using pubsub.get_message(). Behind the
scenes, get_message() uses the system's 'select' module to quickly poll
the connection's socket. If there's data available to be read,
get_message() will read it, format the message and return it or pass it
to a message handler. If there's no data to be read, get_message() will
immediately return None. This makes it trivial to integrate into an
existing event loop inside your application.

.. code:: python

   >>> while True:
   >>>     message = p.get_message()
   >>>     if message:
   >>>         # do something with the message
   >>>     time.sleep(0.001)  # be nice to the system :)

Older versions of redis-py only read messages with pubsub.listen().
listen() is a generator that blocks until a message is available. If
your application doesn't need to do anything else but receive and act on
messages received from redis, listen() is an easy way to get up an
running.

.. code:: python

   >>> for message in p.listen():
   ...     # do something with the message

The third option runs an event loop in a separate thread.
pubsub.run_in_thread() creates a new thread and starts the event loop.
The thread object is returned to the caller of run_in_thread(). The
caller can use the thread.stop() method to shut down the event loop and
thread. Behind the scenes, this is simply a wrapper around get_message()
that runs in a separate thread, essentially creating a tiny non-blocking
event loop for you. run_in_thread() takes an optional sleep_time
argument: how long each iteration of the loop waits on the socket for a
message, so an idle thread notices thread.stop() within that time. A handler
that is still running, or a reconnect in progress, delays it;
thread.stop(timeout=...) bounds the wait.
The default of 0 polls without waiting and keeps a CPU core busy.

Note: Since we're running in a separate thread, there's no way to handle
messages that aren't automatically handled with registered message
handlers. Therefore, redis-py prevents you from calling run_in_thread()
if you're subscribed to patterns or channels that don't have message
handlers attached.

.. code:: python

   >>> p.subscribe(**{'my-channel': my_handler})
   >>> thread = p.run_in_thread(sleep_time=0.1)
   # the event loop is now running in the background processing messages
   # when it's time to shut it down...
   >>> thread.stop()

run_in_thread also supports an optional exception handler, which lets
you catch exceptions that occur within the worker thread and handle them
appropriately. The exception handler will take as arguments the
exception itself, the pubsub object, and the worker thread returned by
run_in_thread.

.. code:: python

   >>> p.subscribe(**{'my-channel': my_handler})
   >>> def exception_handler(ex, pubsub, thread):
   >>>     print(ex)
   >>>     thread.stop()
   >>> thread = p.run_in_thread(exception_handler=exception_handler)

A PubSub object adheres to the same encoding semantics as the client
instance it was created from. Any channel or pattern that's unicode will
be encoded using the encoding specified on the client before being sent
to Redis. If the client's decode_responses flag is set the False (the
default), the 'channel', 'pattern' and 'data' values in message
dictionaries will be byte strings (str on Python 2, bytes on Python 3).
If the client's decode_responses is True, then the 'channel', 'pattern'
and 'data' values will be automatically decoded to unicode strings using
the client's encoding.

PubSub objects remember what channels and patterns they are subscribed
to. In the event of a disconnection such as a network error or timeout,
the PubSub object will re-subscribe to all prior channels and patterns
when reconnecting. Messages that were published while the client was
disconnected cannot be delivered. When you're finished with a PubSub
object, call its .close() method to shutdown the connection.

.. code:: python

   >>> p = r.pubsub()
   >>> ...
   >>> p.close()

The PUBSUB set of subcommands CHANNELS, NUMSUB and NUMPAT are also
supported:

.. code:: python

   >>> r.pubsub_channels()
   [b'foo', b'bar']
   >>> r.pubsub_numsub('foo', 'bar')
   [(b'foo', 9001), (b'bar', 42)]
   >>> r.pubsub_numsub('baz')
   [(b'baz', 0)]
   >>> r.pubsub_numpat()
   1204

Sharded pubsub
~~~~~~~~~~~~~~

`Sharded pubsub <https://redis.io/docs/interact/pubsub/#:~:text=Sharded%20Pub%2FSub%20helps%20to,the%20shard%20of%20a%20cluster.>`_ is a feature introduced with Redis 7.0, and fully supported by redis-py as of 5.0. It helps scale the usage of pub/sub in cluster mode, by having the cluster shard messages to nodes that own a slot for a shard channel. Here, the cluster ensures the published shard messages are forwarded to the appropriate nodes. Clients subscribe to a channel by connecting to either the master responsible for the slot, or any of its replicas.

This makes use of the `SSUBSCRIBE <https://redis.io/commands/ssubscribe>`_ and `SPUBLISH <https://redis.io/commands/spublish>`_ commands within Redis.

The following, is a simplified example:

.. code:: python

    >>> from redis.cluster import RedisCluster, ClusterNode
    >>> r = RedisCluster(startup_nodes=[ClusterNode('localhost', 6379), ClusterNode('localhost', 6380)])
    >>> p = r.pubsub()
    >>> p.ssubscribe('foo')
    >>> # assume someone sends a message along the channel via a publish
    >>> message = p.get_sharded_message()

Similarly, the same process can be used to acquire sharded pubsub messages, that have already been sent to a specific node, by passing the node to get_sharded_message:

.. code:: python

    >>> from redis.cluster import RedisCluster, ClusterNode
    >>> first_node = ClusterNode['localhost', 6379]
    >>> second_node = ClusterNode['localhost', 6380]
    >>> r = RedisCluster(startup_nodes=[first_node, second_node])
    >>> p = r.pubsub()
    >>> p.ssubscribe('foo')
    >>> # assume someone sends a message along the channel via a publish
    >>> message = p.get_sharded_message(target_node=second_node)

The client keeps shard subscriptions attached to the node that currently owns
their slot, so a slot migration or a lost node needs no handling in application
code. When a poll on one node fails, or a node replies ``MOVED`` because it no
longer owns the slot, the client re-routes the affected shard channels to their
new owner and keeps polling the remaining nodes.

**Important**: this affects what ``get_sharded_message()`` raises.

- A connection failure on a single node is no longer surfaced while other nodes
  can still deliver. ``get_sharded_message()`` returns the messages those nodes
  hold, and raises only when every node polled in the pass failed.
- A ``MOVED`` reply is handled internally and no longer raised. The channels the
  former owner no longer serves are re-subscribed on the new owner.
- Code that caught an exception from ``get_sharded_message()`` only to tear the
  subscription down and re-subscribe is no longer needed.
- ``get_sharded_message(target_node=...)`` is unchanged: a caller that names a
  single node still gets its connection errors raised.


Monitor
~~~~~~~

redis-py includes a Monitor object that streams every command processed
by the Redis server. Use listen() on the Monitor object to block until a
command is received.

.. code:: python

   >>> r = redis.Redis(...)
   >>> with r.monitor() as m:
   >>>     for command in m.listen():
   >>>         print(command)
