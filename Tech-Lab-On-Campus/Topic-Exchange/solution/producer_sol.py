import pika
import os
import sys
from producer_interface import mqProducerInterface

class mqProducer(mqProducerInterface):
    global connection, channel

    def __init__(self, routing_key: str, exchange_name: str) -> None:
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        self.setupRMQConnection()
        

    def setupRMQConnection(self) -> None:
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(
            exchange=self.exchange_name, exchange_type="topic"
        )
    
    def publishOrder(self, message: str) -> None:
        self.channel.basic_publish(
            exchange = self.exchange_name,
            routing_key = self.routing_key,
            body= message,
        )
        # self.channel.queue_declare(queue=self.routing_key)
        # self.channel.queue_bind(
        #     queue= self.routing_key,
        #     routing_key= self.routing_key,
        #     exchange= self.exchange_name,
        # )

    def __del__(self) -> None:
        print("Success!")
        self.channel.close()
        self.connection.close()

        
    


