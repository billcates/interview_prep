"""
# Definition for a Node.
class Node:
    def __init__(self, val = 0, neighbors = None):
        self.val = val
        self.neighbors = neighbors if neighbors is not None else []
"""

from typing import Optional
class Solution:
    def cloneGraph(self, node: Optional['Node']) -> Optional['Node']:
        if not node:
            return None
        dt={}
        def dfs(n):
            if n in dt:
                return dt[n]
            new=Node(val=n.val)
            dt[n]=new
            for nei in n.neighbors:
                new.neighbors.append(dfs(nei))
            return new

        return dfs(node)