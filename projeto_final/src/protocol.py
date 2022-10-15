import os
import json
import socket
from tkinter import Image
import errno

from numpy.core.multiarray import empty

HEADER_SIZE = 4

class Message:
    """Base message."""
    def __init__(self, type):
        self.type = type

class ImageRequest(Message):
    """Message to request an image."""
    def __init__(self, path, client_request=True):
        super().__init__("ImageRequest")
        self.path = path
        self.client_request = client_request
        
    def __repr__(self):
        return {"type": self.type, "path": self.path, "client_request": self.client_request}

class ImageReply(Message):
    """Message containing requested image."""
    def __init__(self, path):
        super().__init__("ImageReply")
        self.path = path
        
    def __repr__(self):
        return {"type": self.type, "path": self.path}

class ImageRemove(Message):
    """Message for an image to be deleted."""
    def __init__(self, path):
        super().__init__("ImageRemove")
        self.path = path

    def __repr__(self):
        return {"type": self.type, "path": self.path}

class ListRequest(Message):
    """Message that request the network's list of images."""
    def __init__(self):
        super().__init__("ListRequest")
    
    def __repr__(self):
        return {"type": self.type}

class ListReply(Message):
    """Message that contains a list of images."""
    def __init__(self, lst):
        super().__init__("ListReply")
        self.lst = lst
    
    def __repr__(self):
        return {"type": self.type, "lst": self.lst}

class NotifyMain(Message):
    """Message that let's main node know which images the node has."""
    def __init__(self, addr, dic, size):
        super().__init__("NotifyMain")
        self.addr = addr
        self.dic = dic
        self.size = size
    
    def __repr__(self):
        return {"type": self.type, "addr": self.addr, "dic": self.dic, "size": self.size}

class ImagesNodes(Message):
    def __init__(self, dic, nextMain):
        super().__init__("ImagesNodes")
        self.dic = dic
        self.nextMain = nextMain

    def __repr__(self):
        return {"type": self.type, "dic": self.dic, "next": self.nextMain}

class Path:
    """Image Path."""
    path = ""


class DaemonProto:
    @classmethod
    def image_request(cls, path, client_request=True) -> ImageRequest:
        return ImageRequest(path, client_request)

    @classmethod
    def image_reply(cls, path) -> ImageReply:
        return ImageReply(path)

    @classmethod
    def image_remove(cls, path) -> ImageRemove:
        return ImageRemove(path)

    @classmethod
    def list_request(cls) -> ListRequest:
        return ListRequest()

    @classmethod
    def list_reply(cls, lst) -> ListReply:
        return ListReply(lst)

    @classmethod
    def notify_main(cls, addr, dic, size) -> NotifyMain:
        return NotifyMain(addr, dic, size)

    @classmethod
    def images_nodes(cls, dic, nextMain) -> ImagesNodes:
        return ImagesNodes(dic, nextMain)

    @classmethod
    def send_msg(cls, conn: socket, msg: Message):
        print("sent", msg.type)
        json_msg = json.dumps(msg.__repr__()).encode('utf-8')                # json the msg
        conn.send(len(json_msg).to_bytes(HEADER_SIZE, 'big'))        # send header with msg size
        conn.send(json_msg)                                          # send pickle msg

    @classmethod
    def recv_msg(cls, conn: socket):
        try:
            data = conn.recv(HEADER_SIZE)
        except socket.error as error:
            if error.errno == errno.WSAECONNRESET:
                return None
        header = int.from_bytes(data, 'big')      # receive header with message size
        if header == 0: return None

        payload = conn.recv(header).decode('utf-8')                 # receive pickle msg
        if payload == "": return None
        msg = json.loads(payload)                                 # unjson the msg

        print("got", msg["type"])
        print(msg.__repr__())

        if msg["type"] == "ImageRequest":
            return cls.image_request(msg["path"], msg["client_request"])
        elif msg["type"] == "ImageReply":
            return cls.image_reply(msg["path"])
        elif msg["type"] == "ImageRemove":
            return cls.image_remove(msg["path"])
        elif msg["type"] == "ListRequest":
            return cls.list_request()
        elif msg["type"] == "ListReply":
            return cls.list_reply(msg["lst"])
        elif msg["type"] == "NotifyMain":
            return cls.notify_main(msg["addr"], msg["dic"], msg["size"])
        elif msg["type"] == "ImagesNodes":
            return cls.images_nodes(msg["dic"], msg["next"])
        else:
            print("mankings tu tens que endireitar esta velhisse")  # error if it got here lol
            return None


    @classmethod
    def send_img(cls, connection: socket, path: Path):
        """Sends through a connection a Message object."""
        size = os.path.getsize(path)
        connection.send(size.to_bytes(HEADER_SIZE, 'big')) # header with img size

        with open(path, "rb") as f:
            print("Sending image...")
            while True:
                # read the bytes from the file
                bytes_read = f.read(4096)
                if not bytes_read:
                    # file transmitting is done
                    break
                connection.sendall(bytes_read)

    @classmethod
    def recv_img(cls, connection: socket, path: Path):
        """Receives through a connection a Message object."""
        m = connection.recv(HEADER_SIZE)
        size = int.from_bytes(m, "big")
        remaining_size = size

        with open(path, "wb") as f:
            print("Receiving image...")
            while remaining_size > 0:
                print('.', end="")
                bytes_read = connection.recv(4096)
                f.write(bytes_read)
                remaining_size = remaining_size - len(bytes_read)
            print()