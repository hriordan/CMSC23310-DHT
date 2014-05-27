# main.py
"""
  This is responsible for parsing arguments and initializing the new node.
"""

import parse
import node
print("huh")
args = parse.NodeArgs()
n = node.Node(args.name, args.peers, 0).start()
print n.getName()
print n.getPeers()
print n.getPos()
