import time
import os
import webbrowser
from sys import platform
from socket import *
from src.daemon import Daemon

from src.protocol import DaemonProto

class Client:
    daemon_sock = socket(AF_INET, SOCK_STREAM)

    if platform == "linux" or platform == "linux2":
        firefox_sh = '/usr/lib/firefox/firefox.sh'
        webbrowser.register('firefox', None, webbrowser.BackgroundBrowser(firefox_sh))
        browser = webbrowser.get('firefox')
    elif platform == "win32":
        iexplore = os.path.join(os.environ.get("PROGRAMFILES", "C:\\Program Files"), "Internet Explorer\\IEXPLORE.EXE")
        browser = webbrowser.get(iexplore)

    def __init__(self, host, port):
        self.host = host
        self.port = int(port)
        self.connect()
        self.loop()

    def connect(self):
        self.daemon_sock.connect((self.host, self.port))
        print('This client has entered the network\n')

    def loop(self):
        try:
            while True:
                print('What is your wish? List / View [imagePath]')
                data = input().split()
                option = data[0].lower()
                if option == "list":
                    msg = DaemonProto.list_request()
                elif option == "view":
                    img_path = data[1]
                    msg = DaemonProto.image_request(img_path, True)
                DaemonProto.send_msg(self.daemon_sock, msg)
                self.recv()
        except KeyboardInterrupt:
            print("Caught KeyboardInterrupt. Exiting...")

    def recv(self):
        msg = DaemonProto.recv_msg(self.daemon_sock)
        print(msg.__repr__())
        if msg:
            if msg.type == 'ImageReply':
                self.browser.open(msg.path)
                time.sleep(1)
                os.remove(msg.path)
            elif msg.type == 'ListReply':
                for img_path in msg.lst:
                    print("  >", img_path)
                print()

        else:
            print("Daemon is now dead.")
            self.daemon_sock.close()