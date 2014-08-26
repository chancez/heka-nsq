Heka-Nsq
========

This is a set of consumer/producer input/output plugins for
[Heka](http://hekad.readthedocs.org/).

To use Heka-Nsq you need to build Heka with the plugin. You can do this by
adding the following lines to `cmake/plugin_loader.cmake`:


````
hg_clone(https://code.google.com/p/snappy-go default)
git_clone(https://github.com/mreiferson/go-snappystream master)
git_clone(https://github.com/bitly/go-nsq master)
git_clone(https://github.com/bitly/go-hostpool master)
add_external_plugin(git https://github.com/ecnahc515/heka-nsq master)
````

Refer to Heka's offical [Building External Plugins]
(http://hekad.readthedocs.org/en/latest/installing.html#build-include-externals)
docs for more details.


Configuration
=============

NsqInput
--------

Connects to a list of `nsqlookupd` servers and a list of `nsqd` servers, and
begins consuming a particular topic with the specified channel.

Config:

* lookupd_http_addresses (list of strings): A list of nsqlookupd http addresses
which the consumer will connect to for discovering which nsqd nodes contain the
topic its subscribing to.
* nsqd_tcp_addresses (list of strings): A list of nsqd tcp addresses which the
consumer will connect to directly.
* topic (string): The topic to subscribe to.
* channel (string): The channel to use.
* decoder (string): The decoder name to transform a raw message body into a
structured hekad message.
* use_msgbytes (bool, optional): A true value here will cause a NatsInput to
treat the Nats message body as a protobuf encoding of an entire Heka message, as
opposed to just a message payload. Defaults to true if a ProtobufDecoder is in
use, false otherwise. The default behavior will almost always be correct, you
should only explicitly set this if you know how Heka's decoding works
internally.
* use_tls (bool): Specifies whether or not SSL/TLS encryption should be used
for the TCP connections. Defaults to false.
* tls (TlsConfig): A sub-section that specifies the settings to be used for any
SSL/TLS encryption. This will only have any impact if use_tls is set to true.
See [Configuring TLS][tls].
* read_timeout (int, optional): Deadline for network reads in milliseconds.
Defaults to 60 seconds.
* write_timeout (int, optional): Deadline for network writes in milliseconds.
Defaults to 1 second.
* lookupd_poll_interval (int, optional): Duration between polling lookupd for
new producers in milliseconds. Defaults to 60 seconds.
* lookupd_poll_jitter (float, optional): Fractional jitter to add to the
lookupd pool loop. this helps evenly distribute requests even if multiple
consumers restart at the same time. Defaults to 0.3.
* max_requeue_delay (int, optional): Maximum duration in milliseconds when
REQueueing (for doubling of deferred requeue). Defaults to 15 minutes.
* default_requeue_delay (int, optional): Default duration in milliseconds for
REQueueing. Defaults to 90 seconds.
* backoff_multiplier (int, optional): Unit of time in milliseconds for
calculating consumer backoff. Defaults to 1 second.
* max_attempts (int, optional): Maximum number of times a consumer will attempt
to process a message before giving up. Defaults to 5.
* low_rdy_idle_timeout (int, optional): Amount of time in milliseconds to wait
for a message from a producer when in a state where RDY counts are re-distributed
(ie. max_in_flight < num_producers). Defaults to 10 seconds.
* client_id (string, optional): Client id sent to nsqd representing this client.
Defaults to short hostname.
* hostname (string, optional): Hostname sent to nsqd representing this client.
* user_agent (string, optional): User agent sent to nsqd representing this client.
defaults to "<client_library_name>/<version>".
* heartbeat_interval (int, optional): Duration of time in milliseconds between
heartbeats. This must be less than `read_timeout`. Defaults to 30 seconds.
* sample_rate (int, optional): Integer percentage to sample the channel
(requires nsqd 0.2.25+).
* deflate (bool, optional): Use deflate compression algorithm.
* deflate_level (int, optional): Sets deflate level. Defaults to 6.
* snappy (bool, optional): Use snappy compression/decompression.
* output_buffer_size (int, optional): Size of the buffer (in bytes) used by nsqd
for buffering writes to this connection. Defaults to 16384.
* output_buffer_timeout (int, optional): Timeout in milliseconds used by nsqd
before flushing buffered writes (set to 0 to disable). Defaults to 250 milliseconds.
* max_in_flight (int, optional): Maximum number of messages to allow in flight.
Defaults to 1.
* max_backoff_duration (int, optional): Maximum amount of time in milliseconds
to backoff when processing fails 0 == no backoff. Defaults to 2 minutes.
* msg_timeout (int, optional): The server-side message timeout in milliseconds
for messages delivered to this client.
* auth_secret (string, optional): Secret for nsqd authentication
(requires nsqd 0.2.29+).



[tls]: http://hekad.readthedocs.org/en/latest/tls.html#tls "configuring tls"

