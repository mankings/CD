from src.daemon import Daemon
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("image_folder", help="folder where the images are stored", type=str, default=None)
    parser.add_argument("-m", "--main", help="set this node as the main daemon node", action="store_true")

    args = parser.parse_args()

    daemon = Daemon(args.image_folder, args.main)