import argparse
from project import *

# add_project(dest=[local])
# update_project()
# remove_project()

parser = argparse.ArgumentParser(description="GREENCap CLI")

parser.add_argument("-c", "--command",
                    choices=["add_project", "update_project", "remove_project"],
                    help="Possible commands:/n add_project/n update_project/n remove_project",
                    type=str)
parser.add_argument("-n", "--name",
                    help="Project Name",
                    type=str)
parser.add_argument("-d", "--destination",
                    help="Destination for project creation",
                    type=str)

args = parser.parse_args()

def process_args():
    if args.command:
        if args.command == "add_project":
            pass
        if args.command == "update_project":
            pass
        if args.command == "remove_project":
            pass
