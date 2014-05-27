# main.py
"""
  This is responsible for parsing arguments and initializing the new node.
"""

import parse
import node

args = parse.NodeArgs()

n = node.Node(args.name, args.peers, 0)
