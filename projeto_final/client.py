from src.client import Client
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("host", help="host of the client socket", type=str, default=None)
    parser.add_argument("port", help="port of the client socket", type=str, default=None)

    args = parser.parse_args()

    client = Client(args.host, args.port)
