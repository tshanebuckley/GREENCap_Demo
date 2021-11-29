import argparse
#from project import *


parser = argparse.ArgumentParser(description="GREENCap CLI")

parser.add_argument("-n", "--name",
                    help="name",
                    metavar="",
                    required=True)
parser.add_argument("-u", "--url",
                    help="url",
                    metavar="",
                    required=True)
parser.add_argument("-l", "--local",
                    help="local",
                    metavar="",
                    required=True)

args = parser.parse_args()


if __name__ == '__main__':
    name = args.name
    url = args.url
    local = args.local
    print("Adding project with the following credentials...")
    print("user: %s \nurl: %s \nlocal: %s" % (name, url, local))
    #user_project(name=name, url=url, local=local)
