import pika
import os
from consumer_interface import mqConsumerInterface

class mqConsumer(mqConsumerInterface): 


    def __init__(self, binding_key: str, exchange_name: str, queue_name: str) -> None:
        #binding_key=bindingKey,exchange_name="Tech Lab Topic Exchange",queue_name=queueName
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name  = queue_name
        self.setupRMQConnection()
        

    def setupRMQConnection(self) -> None:
        # Establish connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)

        # Establish Channel
        self.channel = self.connection.channel()

        # Declare queue and exchange
        self.createQueue(queueName=self.queue_name)
        self.channel.exchange_declare(exchange=self.exchange_name)
        #self.bindQueueToExchange(self, queueName=self.queue_name, )
        self.channel.queue_bind(
            queue= self.queue_name,
            routing_key= self.binding_key,
            exchange=self.exchange_name,
        )
        self.channel.basic_consume(
            self.queue_name, self.callBack, auto_ack=False
        )


    def bindQueueToExchange(self, queueName: str, topic: str) -> None:
        # Bind binding key to queue on exchange
        pass

    def createQueue(self, queueName: str) -> None:
        self.channel.queue_declare(queue=queueName)
        # Set-up callback function
    
    def callBack(self, method_frame, body) -> None:
        self.channel.basic_ack(method_frame.delivery_tag, False)
        print(body)

    def __del__(self) -> None:
        self.channel.close()
        self.connection.close()
        print("Success")

        
