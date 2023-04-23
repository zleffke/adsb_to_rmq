import pika
import json
import logging
import time
from threading import Thread


class BrokerConnector(object):
    """BrokerConnector provides basic functionality to support the connection
    "sequence" of a typical Pika to RabbitMQ connection.  Individual
    functions could and should be overridden if alternate or additional
    functionality is desired.  Derived classes for consumers and producers
    would need to implement the final steps in the connection process.
    Specifically, both producers and consumers would pick up at the
    'on_exchange_declareok' function and re-implement that to customize their
    behaviour.  Since the producer doesn't need to declare a queue it could
    simply start publishing to the exchange.  The consumer on the other hand
    would continue with the declaration and binding of a queue.
    """

    def __init__(self, cfg, loggername=None):
        """Create a new instance of the connector class, passing in the RabbitMQ
        connection URL and exchange/routing information.  The specified exchange
        will be created if it does not exist.

        See the config.yaml file for the definition of the configuration parameters.
        """
        # Store the passed-in parameters in local (class) data...
        self.cfg = cfg
        self.credentials = pika.PlainCredentials(cfg['user'],
                                                 cfg['pass'])
        self.parameters = pika.ConnectionParameters(host = cfg['ip'],
                                                    port = cfg['port'],
                                                    virtual_host = cfg['vhost'],
                                                    credentials = self.credentials)
        self.exchange      = cfg['exchange']
        self.exchange_type = cfg['exch_type']
        self.exch_durable  = cfg['durable']
        self.loggername    = loggername
        self.logger        = logging.getLogger(self.loggername)
        self.retry_wait    = cfg['retry_wait']
        # ...and set up the storage for the connection data
        self.connection    = None
        self.channel       = None
        self.closing       = False
        self.connected     = False


    def run(self):
        """Run the BrokerConnector-derived instance by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.
        """
        self.connection = self.connect()
        #print(self.connection)
        self.connection.ioloop.start()

    def connect(self):
        """Connect to the RabbitMQ server and return the connection handle.
        When the connection is established, on_connection_open will be called.
        """
        if self.loggername is not None:
            #logging.getLogger(self.loggername).debug('Connecting to %s', self.url)
            self.logger.debug('Connecting to [{:s}:{:d}]'.format(self.cfg['ip'], self.cfg['port']))
        #return pika.SelectConnection(pika.URLParameters(self.url),
        #                             self.on_connection_open,
        #                             stop_ioloop_on_close=False)
        return pika.SelectConnection(parameters=self.parameters,
                                     on_open_callback=self.on_connection_open,
                                     on_open_error_callback=self.on_connection_error)

    def on_connection_error(self, conn, e):
        self.connected = False
        self.connection = conn
        if self.loggername is not None:
            self.logger.debug('Connection Error: {:s}'.format(e))
        time.sleep(self.retry_wait)
        self.reconnect()

    def on_connection_open(self, conn):
        """Called when the connection to the RabbitMQ server has been
        established.
        """
        self.connected = True
        # print "Connection to rabbitmq server: {:s}:{:d} has been established...".format(self.cfg['ip'], self.cfg['port'])
        if self.loggername is not None:
            self.logger.debug("Connection to rabbitmq server: {:s}:{:d} has been established...".format(self.cfg['ip'], self.cfg['port']))
        self.add_on_connection_close_callback()
        self.open_channel()

    def add_on_connection_close_callback(self):
        """Add an "on close" callback that will be called whenever
        the server closes the connection unexpectedly.
        """
        if self.loggername is not None:
            self.logger.debug('Adding connection close callback')
        self.connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code):
        """Called when the server connection is closed unexpectedly. Try
        to reconnect after a small delay.
        """
        self.connected = False
        self.channel = None
        if self.loggername is not None:
            self.logger.warning('Connection closed: {:s}'.format(str(reply_code)))
        time.sleep(self.retry_wait)
        self.reconnect()

    def reconnect(self):
        """Called by the IOLoop timer if the connection is closed. See the
        on_connection_closed method.
        """
        # This is the old connection IOLoop instance, stop its ioloop
        self.connection.ioloop.stop()
        #Start a new connection
        self.connection = self.connect()
        self.connection.ioloop.start()


    def open_channel(self):
        """Open a new channel with the server and provide the "on open" callback.
        """
        if self.loggername is not None:
            self.logger.debug('Creating a new channel')
        self.connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """Called when the channel has been opened (we registered it in the
        channel open call).  We'll also declare the exchange.
        """
        if self.loggername is not None:
            self.logger.debug('Channel opened')
        self.channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange()

    def add_on_channel_close_callback(self):
        """Register a callback for when/if the server closes the channel.
        """
        if self.loggername is not None:
            self.logger.debug('Adding channel close callback')
        self.channel.add_on_close_callback(callback=self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code):
        """Called when the server closes the channel.
        """
        if self.loggername is not None:
            self.logger.warning('Channel %i was closed: (%s)',
                              channel, reply_code)
        if not self.closing:
            self.connection.close()

    def setup_exchange(self):
        """Declare the exchange on the server. We'll register a "declare OK"
        callback which will be called when at the completion of the exchange
        declaration on the server.
        """
        if self.loggername is not None:
            self.logger.debug('Declaring exchange "%s"', self.exchange)
        self.channel.exchange_declare(callback = self.on_exchange_declareok,
                                      exchange = self.exchange,
                                      exchange_type = self.exchange_type,
                                      durable = self.exch_durable)

    def on_exchange_declareok(self, frame):
        """Called when the server has finished the creation of the exchange.
        """
        if self.loggername is not None:
            self.logger.debug('Exchange declared successfully')

    def close_channel(self):
        """Close the channel with the server cleanly.
        """
        if self.loggername is not None:
            self.logger.debug('Closing the channel')
        if self.channel:
            self.channel.close()

    def stop(self):
        """Cleanly shutdown the connection to the server.
        """
        if self.loggername is not None:
            self.logger.debug('Stopping Broker Connector')
        self.closing = True
        self.connection.ioloop.stop()

    def close_connection(self):
        """Close the connection to server"""
        if self.loggername is not None:
            self.logger.debug('Closing connection')
        self.connection.close()
