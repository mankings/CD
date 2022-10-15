"""Message Broker"""
import enum
from typing import Dict, List, Any, Tuple
import socket
import selectors

from src.protocol import PubSubProtocol


class Serializer(enum.Enum):
    """Possible message serializers."""

    JSON = 0
    XML = 1
    PICKLE = 2


class Broker:
    """Implementation of a PubSub Message Broker."""
    
    def __init__(self):
        """Initialize broker."""
        self.canceled = False
        self._host = "localhost"
        self._port = 5000
        self.broker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)    # returns new socket and addr.
        self.broker.bind((self._host, self._port))
        self.sel = selectors.DefaultSelector()
        self.broker.listen()

        self.topics = []            # [topic1, topic2, topic3, ...] -> stores existing topics
        self.messages = {}          # {topic1: lastMessage1, topic2: lastMessage2, ...} -> stores last message in each
        self.serialTypes = {}       # {consumer1: serializationType1, consumer2: serializationType2, ...} -> stores each consumers searialization type
        self.subscriptions = {}     # {topic1: [consumer1a, consumer1b, ...], topic2: [consumer2a, consumer 2b, ...], ...} -> stores all subscriptions

        self.sel.register(self.broker, selectors.EVENT_READ, self.accept)

    def accept(self, broker, mask):
        """Accept a connection and store it's serialization type."""
        conn, addr = broker.accept()                                        # accept connection

        self.sel.register(conn, selectors.EVENT_READ, self.read)

    def read(self, conn, mask):
        """Handle further operations"""

        received = PubSubProtocol.recvMsg(conn)

        if received:  # we got a message from PubSubProtocol
            if received.type == "Subscribe":
                self.subscribe(received.topic, conn, self.getSerial(conn))

            elif received.type == "Publish":
                
                self.put_topic(received.topic, received.value)
                if received.topic in self.subscriptions:
                    for key in self.list_subscriptions(received.topic):
                        PubSubProtocol.sendMsg(key[0], self.getSerial(key[0]), received)
                else:
                    self.subscriptions[received.topic] = []

            elif received.type == "TopicListRequest":
                PubSubProtocol.sendMsg(conn, self.serialType, PubSubProtocol.topicListReply(self.list_topics()))

            elif received.type == "CancelSubscription":
                self.unsubscribe(received.topic, conn)

            elif received.type == "Acknowledge" or received["type"] == "Acknowledge":
                self.acknowledge(conn, received.language)

        else:  # we got no message so connection was closed
            self.unsubscribe("", conn)
            self.sel.unregister(conn)
            conn.close()

    def list_topics(self) -> List[str]:
        """Returns a list of strings containing all topics containing values."""
        lst = []
        for topic in self.messages.keys():
            if self.messages[topic] is not None:
                lst.append(topic)
        return lst

    def get_topic(self, topic):
        """Returns the currently stored value in topic."""
        if topic in self.messages:
            return self.messages[topic]
        else:
            return None

    def put_topic(self, topic, value):
        """Store in topic the value."""

        if topic not in self.topics:
            self.createTopic(topic)

        # store the value as the topic's last message and add topic to topics if it's not there yet
        self.messages[topic] = value

    def list_subscriptions(self, topic: str) -> List[Tuple[socket.socket, Serializer]]:
        """Provide list of subscribers to a given topic."""

        lst = []
        for conn in self.subscriptions[topic]:
            lst.append((conn, self.serialTypes[conn]))

        return lst

    def subscribe(self, topic: str, address: socket.socket, _format: Serializer = None):
        """Subscribe to topic by client in address."""
        conn = address
        serializationCode = _format

        # if serial type not yet registered, then register it
        if conn not in self.serialTypes:
            self.acknowledge(conn, serializationCode)

        # subscribe given topic
        if topic in self.topics:
            if topic not in self.subscriptions:
                self.createTopic(topic)

            if conn not in self.subscriptions[topic]:
                self.subscriptions[topic].append(conn)
            
            if topic in self.messages and self.messages[topic] is not None:
                PubSubProtocol.sendMsg(conn, self.getSerial(conn), PubSubProtocol.publish(topic, self.messages[topic]))
            return

        # if it got here, topic doesn't exist, so create it and re-run subscribe
        else:
            self.put_topic(topic, None)
            self.subscribe(topic, conn, serializationCode)

    def unsubscribe(self, topic, address):
        """Unsubscribe to topic by client in address."""
        conn = address
        
        if topic != "":     # unsub from specific topic and all subtopics
            for t in self.topics:
                if(t.startswith(topic)):
                    self.subscriptions[t].remove(conn)
        else:               # unsub from all topics
            for t in self.topics:
                if(conn in self.subscriptions[t]):
                    self.subscriptions[t].remove(conn)

    def acknowledge(self, conn, serializationCode):
        """Acknowledge new connection and its serialization type."""
        if serializationCode == 0 or serializationCode == Serializer.JSON or serializationCode == None:
            self.serialTypes[conn] = Serializer.JSON
        elif serializationCode == 1 or serializationCode == Serializer.XML:
            self.serialTypes[conn] = Serializer.XML
        elif serializationCode == 2 or serializationCode == Serializer.PICKLE:
            self.serialTypes[conn] = Serializer.PICKLE

    def createTopic(self, topic):
        self.topics.append(topic)
        self.subscriptions[topic] = []

        for t in self.topics:
            if(topic.startswith(t)):
                for consumer in self.subscriptions[t]:
                    if consumer not in self.subscriptions[topic]:
                        self.subscriptions[topic].append(consumer)
    
    def getSerial(self, conn):
        if conn in self.serialTypes:
            return self.serialTypes[conn]
            
    def run(self):
        """Run until canceled."""
        while not self.canceled:
            events = self.sel.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)