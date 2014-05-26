# parse.py

import argparse
"""
parser = argparse.ArgumentParser()
parser.add_argument("-n","--node-name", dest = "name",
                    help = "Name of the node")
parser.add_argument("-pep", "--pub-endpoint", dest = "pep",
                    help = "public endpoint")
parser.add_argument("-rep", "--router-endpoint", dest = "rep",
                    help = "router endpoint")
parser.add_argument("-pf", "--peers", dest = "peers",
                    help = "A  containing the peer list")
args = parser.parse_args()
bundle = {}
if args.name != None:
    print args.name
if args.pep != None:
    print args.pep
if args.rep != None:
    print args.rep
if args.peers != None:
"""
class NodeArgs(object):
    def __init__(self):
        """ Parses the command line arguments. """
        parser = argparse.ArgumentParser()
        parser.add_argument("-n", "--node-name", dest = "name",
                            help = "Name of the node")
        parser.add_argument("-pep", "--pub-endpoint", dest = "pep",
                            help = "public endpoint")
        parser.add_argument("-rep", "--router-endpoint", dest = "rep",
                            help = "router endpoint")
        parser.add_argument("-pf", "--peers", dest = "peers",
                            help = "The comma-separated peer list.")

        args = parser.parse_args()
        self.name = args.name
        self.pep = args.pep
        self.rep = args.rep
        self.peers = args.peers.split(",")
