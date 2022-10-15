import json
import pickle
import xml.etree.ElementTree as et
import enum
import socket


class Serializer(enum.Enum):
    """Possible serializers."""

    # we use json as the default serializer, and convert to one of the other two if needed
    JSON = 0
    XML = 1
    PICKLE = 2

class Message:
    """Base message."""

    def __init__(self, type):
        self.type = type

class Subscribe(Message):
    """Message to subscribe a topic."""

    def __init__(self, topic):
        super().__init__("Subscribe")
        self.topic = topic

    def __repr__(self):
        return f'{{"type": "{self.type}", "topic": "{self.topic}"}}'

    def toXML(self):
        return "<?xml version=\"1.0\"?><data type=\"{}\" topic=\"{}\"></data>".format(self.type, self.topic)

    def toPickle(self):
        return {"type": self.type, "topic": self.topic}

class Publish(Message):
    """Message to publish on a topic"""

    def __init__(self, topic, value):
        super().__init__("Publish")
        self.topic = topic
        self.value = value

    def __repr__(self):
        return f'{{"type": "{self.type}", "topic": "{self.topic}", "value": "{self.value}"}}'

    def toXML(self):
        return "<?xml version=\"1.0\"?><data type=\"{}\" topic=\"{}\" value=\"{}\"></data>".format(
            self.type, self.topic, self.value)

    def toPickle(self):
        return {"type": self.type, "topic": self.topic, "value": self.value}

class TopicListRequest(Message):
    """Message to request the topic list."""

    def __init__(self):
        super().__init__("TopicListRequest")

    def __repr__(self):
        return f'{{"type": "{self.type}"}}'

    def toXML(self):
        return "<?xml version=\"1.0\"?><data type=\"{}\"></data>".format(self.type)

    def toPickle(self):
        return {"type": self.type}

class TopicListReply(Message):
    """Message to reply with the topic list."""
    
    def __init__(self, lst):
        super().__init__("TopicListReply")
        self.lst = lst

    def __repr__(self):
        return f'{{"type": "{self.type}", "lst": "{self.lst}"}}'

    def toXML(self):
        return "<?xml version=\"1.0\"?><data type=\"{}\" lst=\"{}\"></data>".format(self.type, self.lst)

    def toPickle(self):
        return {"type": self.type, "lst": self.lst}

class CancelSubscription(Message):
    """Message to cancel a subscription on a topic"""

    def __init__(self, topic):
        super().__init__("CancelSubscription")
        self.topic = topic

    def __repr__(self):
        return f'{{"type": "{self.type}", "topic": "{self.topic}"}}'

    def toXML(self):
        return "<?xml version=\"1.0\"?><data type=\"{}\" topic=\"{}\"></data>".format(self.type, self.topic)

    def toPickle(self):
        return {"type": self.type, "topic": self.topic}

class Acknowledge(Message):
    """Message to inform broker of your language"""

    def __init__(self, language):
        super().__init__("Acknowledge")
        self.language = language

    def __repr__(self):
        return f'{{"type": "{self.type}", "language": "{self.language}"}}'

    def toXML(self):
        return "<?xml version=\"1.0\"?><data type=\"{}\" language=\"{}\"></data>".format(self.type, self.language)

    def toPickle(self):
        return {"type": self.type, "language": self.language}


class PubSubProtocol:
    @classmethod
    def subscribe(cls, topic) -> Subscribe:
        return Subscribe(topic)

    @classmethod
    def publish(cls, topic, value) -> Publish:
        return Publish(topic, value) 

    @classmethod
    def topicListRequest(cls) -> TopicListRequest:
        return TopicListRequest()

    @classmethod
    def topicListReply(cls, lst) -> TopicListReply:
        return TopicListReply(lst)

    @classmethod
    def cancelSubscription(cls, topic) -> CancelSubscription:
        return CancelSubscription(topic)

    @classmethod
    def acknowledge(cls, language) -> Acknowledge:
        return Acknowledge(language)

    @classmethod
    def sendMsg(cls, conn: socket, serializerCode, msg: Message):
        """Send a message."""

        if serializerCode == None: serializerCode = 0
        if type(serializerCode) == str: serializerCode = int(serializerCode)
        if isinstance(serializerCode, enum.Enum): serializerCode = serializerCode.value

        conn.send(serializerCode.to_bytes(1, 'big'))            # first we send the serializer
        if serializerCode == 0 or serializerCode == Serializer.JSON:
            temp = json.loads(msg.__repr__())
            jsonMsg = json.dumps(temp).encode('utf-8')          # get message in JSON
            conn.send(len(jsonMsg).to_bytes(2, 'big'))          # send header
            conn.send(jsonMsg)                                  # send message
        elif serializerCode == 1 or serializerCode == Serializer.XML:
            xmlMsg = msg.toXML().encode('utf-8')                # get message in XML
            conn.send(len(xmlMsg).to_bytes(2, 'big'))           # send header
            conn.send(xmlMsg)                                   # send message
        elif serializerCode == 2 or serializerCode == Serializer.PICKLE:
            pickleMsg = pickle.dumps(msg.toPickle())            # get message in Pickle
            conn.send(len(pickleMsg).to_bytes(2, 'big'))        # send header
            conn.send(pickleMsg)                                # send message

    @classmethod
    def recvMsg(cls, conn: socket) -> Message:
        """Receive a message."""

        serializerCode = int.from_bytes(conn.recv(1), 'big')    # get serializer
        header = int.from_bytes(conn.recv(2), 'big')            # get header which contains message length
        if header == 0: return None

        try:
            if serializerCode == 0 or serializerCode == Serializer.JSON or serializerCode == None:
                payload = conn.recv(header).decode('utf-8')     # get content with header size
                if payload == "": return None                   
                msg = json.loads(payload)                       # decode content into message
            elif serializerCode == 1 or serializerCode == Serializer.XML:
                payload = conn.recv(header).decode('utf-8')     # get content with header size
                if payload == "": return None
                msg = {}                                        # decode content into message
                root = et.fromstring(payload)
                for element in root.keys():
                    msg[element] = root.get(element)
            elif serializerCode == 2 or serializerCode == Serializer.PICKLE:
                payload = conn.recv(header)                     #  get content with header size
                if payload == "": return None
                msg = pickle.loads(payload)                     # decode content into message


        except json.JSONDecodeError as err:
            raise PubSubProtocolBadFormat(payload)

        if msg["type"] == "Subscribe":
            return cls.subscribe(msg["topic"])
        elif msg["type"] == "Publish":
            return cls.publish(msg["topic"], msg["value"])
        elif msg["type"] == "TopicListRequest":
            return cls.topicListRequest()
        elif msg["type"] == "TopicListReply":
            return cls.topicListReply(msg["lst"])
        elif msg["type"] == "CancelSubscription":
            return cls.cancelSubscription(msg["topic"])
        elif msg["type"] == "Acknowledge":
            return cls.acknowledge(msg["language"])
        else: # error if it got here lol
            print("couldn't parse (?) type")
            return None

class PubSubProtocolBadFormat(Exception):
    """Exception when source message is not PubSubProtocol"""

    def __init__(self, original_msg: bytes=None) :
        """Store original message that triggered exception."""
        self._original = original_msg

    @property
    def original_msg(self) -> str:
        """Retrieve original message as a string."""
        return self._original.decode("utf-8")

