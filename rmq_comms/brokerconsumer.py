import logging
from rmq_comms.brokerconnector import BrokerConnector

class BrokerConsumer(BrokerConnector):
    """BrokerConsumer provides basic functionality to support the connection
    needed for a typical Pika to RabbitMQ consumer connection.  Individual
    functions could be overridden if alternate or additional functionality is
    desired.  Derived classes would override the "process_message" function.
    """

    def __init__(self, cfg, loggername=None):
        """Create a new instance of the consumer class, passing in the RabbitMQ
        connection URL and exchange/routing information.  The specified exchange
        will be created if it does not exist.

        :param str amqp_url:      The Rabbit connection url.
        :param str exchang:       The name of the exchange to interact with.
        :param str exchange_type: The RabbitMQ exchange type.  If the exchange
                                  already exists the type _must_ agree with
                                  the existing value or an error will be thrown.
        :param str queue:         The name of the queue used for interaction.  If
                                  the string is empty (the default) a new, unique
                                  queue will be created by RabbitMQ.
        :routing_key:             The filter/routing key used to connect the
                                  queue to the exchange.
        """
        super(BrokerConsumer, self).__init__(cfg, loggername)

        self.queue = None
        self.routing_key = None

    def on_exchange_declareok(self, frame):
        """Called when the server has finished the creation of the exchange.
        """
        if self.loggername is not None:
            self.logger.debug('Exchange declared successfully')
        self.setup_queue()

    def setup_queue(self):
        """Declare the specified queue on server.  We'll register a "declare OK"
        callback which will be called when at the completion of the queue
        declaration on the server.
        """
        if self.loggername is not None:
            self.logger.debug('Declaring queue "%s"', self.queue)
        try:
            self.channel.queue_declare(queue=self.queue,
                                       callback=self.on_queue_declareok,
                                       auto_delete=True)
        except Exception as e:
            print(e)

    def on_queue_declareok(self, method_frame):
        """Called when the server has finished the creation of the queue.
        We'll bind the just-creted queue to the exchange using the
        objects routing key.  We'll register a "bind complete" callback.
        """

        if self.loggername is not None:
            self.logger.debug('Binding "%s" to "%s" with "%s"',
                              self.exchange, self.queue, self.routing_key)
        self.channel.queue_bind(callback = self.on_bindok,
                                queue = self.queue,
                                exchange = self.exchange,
                                routing_key=self.routing_key)

    def on_bindok(self, unused_frame):
        """Final stage of connection to broker.  Derived classes should override
        this function and perform their real work.
        """
        if self.loggername is not None:
            self.logger.debug('Queue bound by Consumer')
        self.start_consuming()

    def start_consuming(self):
        """Begin receiving messages.  We'll register the on_message function
        as a callback for message receipt.
        """
        if self.loggername is not None:
            self.logger.debug('Registering as consumer with broker')
        self.add_on_cancel_callback()
        self.consumer_tag = self.channel.basic_consume(on_message_callback = self.on_message,
                                                       queue = self.queue)

    def process_message(self, channel, method, properties, body):
        """Function that gets called whenever a message is received.  It is
        anticipated that it will be overridden with the desired functionality
        in a derived class.
        """
        if self.loggername is not None:
            self.logger.debug('Received message %s from %s: %s',
                              method.delivery_tag, properties.app_id, body)

    def add_on_cancel_callback(self):
        """Register a callback that will be invoked if the broker cancels the consumer
        for some reason.
        """
        if self.loggername is not None:
            self.logger.debug('Adding consumer cancellation callback')
        self.channel.add_on_cancel_callback(callback=self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """This callback is called by pika when the broker sends a Basic.Cancel for a
        consumer receiving messages.  Here we'll just close the connection.
        """
        if self.loggername is not None:
            self.logger.debug('Consumer was cancelled remotely, shutting down: %r',
                    method_frame)
        if self.channel:
            self.channel.close()

    def on_message(self, channel, basic_deliver, properties, body):
        """Whenever a message is received this function is called to process it.
        Here we'll just call the "process_message" function then send a ACK.
        That allows derived classes to easily implement their own receive
        functionality by overriding the process_message function and not have
        to worry about ACKs.
        """
        self.process_message(channel, basic_deliver, properties, body)
        self.acknowledge_message(basic_deliver.delivery_tag)

    def acknowledge_message(self, delivery_tag):
        """Send a message ACK to the broker indicating that we've received
        and processed the message.
        """
        if self.loggername is not None:
            self.logger.debug('Acknowledging message %s', delivery_tag)
        self.channel.basic_ack(delivery_tag)

    def stop_consuming(self):
        """Tell the broker that we are done and would like to stop consuming
        messages.  This will send the Basic.Cancel RPC command and makes a clean
        connection break with the broker.  We'll register a callback that will
        be called whne the cancel request is handled.
        """
        if self.channel:
            if self.loggername is not None:
                self.logger.debug('Sending a Basic.Cancel RPC command to RabbitMQ')
            self.channel.basic_cancel(self.on_cancelok, self.consumer_tag)

    def on_cancelok(self, unused_frame):
        """The broker responded with a ACK on our cancle request.  We're free
        to close the connection and clean up.
        """
        if self.loggername is not None:
            self.logger.debug('Broker ACK of consume cancellation')
        self.close_channel()
