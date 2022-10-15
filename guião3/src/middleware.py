"""Middleware to communicate with PubSub Message Broker."""
from collections.abc import Callable
from enum import Enum
from queue import LifoQueue, Empty
import socket
from typing import Any
import json
import pickle
import xml.etree.ElementTree as et

from src.protocol import PubSubProtocol


class MiddlewareType(Enum):
    """Middleware Type."""

    CONSUMER = 1
    PRODUCER = 2


class Queue:
    """Representation of Queue interface for both Consumers and Producers."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        """Create Queue."""        
        self.topic = topic
        self.type = _type
        self.serial = 0
        self.address = (('localhost', 5000))
        self.mwSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.mwSock.connect(self.address)

    def push(self, value):
        """Sends data to broker."""
        if self.type.value == 2:                # if it's a producer
            PubSubProtocol.sendMsg(self.mwSock, self.serial, PubSubProtocol.publish(self.topic, value))

    def pull(self) -> (str, Any):
        """Receives (topic, data) from broker.
        Should BLOCK the consumer!"""
        received = PubSubProtocol.recvMsg(self.mwSock)
        if received is None or received.value is None: return None
        if received.type == "TopicListReply":  
            return received.lst
        else:
            return (received.topic, int(received.value))

    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        PubSubProtocol.sendMsg(self.mwSock, self.serial, PubSubProtocol.topicListRequest())
        #received = callback(PubSubProtocol.recvMsg(self.mwSock))

    def cancel(self):
        """Cancel subscription."""
        PubSubProtocol.sendMsg(self.mwSock, self.serial, PubSubProtocol.cancelSubscription(self.topic))

    def subscribe(self):
        PubSubProtocol.sendMsg(self.mwSock, self.serial, PubSubProtocol.subscribe(self.topic))


class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""
    def __init__(self, topic, _type = MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        self.serial = 0
        
        if _type.value == 1: # if it's a consumer
            PubSubProtocol.sendMsg(self.mwSock, 0, PubSubProtocol.acknowledge(self.serial))
            PubSubProtocol.sendMsg(self.mwSock, self.serial, PubSubProtocol.subscribe(topic))

class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""
    def __init__(self, topic, _type = MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        self.serial = 1
        
        if _type.value == 1: # if it's a consumer
            PubSubProtocol.sendMsg(self.mwSock, 0, PubSubProtocol.acknowledge(self.serial))
            PubSubProtocol.sendMsg(self.mwSock, self.serial, PubSubProtocol.subscribe(topic))

class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""
    def __init__(self, topic, _type = MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        self.serial = 2
        
        if _type.value == 1: # if it's a consumer
            PubSubProtocol.sendMsg(self.mwSock, 0, PubSubProtocol.acknowledge(self.serial))
            PubSubProtocol.sendMsg(self.mwSock, self.serial, PubSubProtocol.subscribe(topic))
        