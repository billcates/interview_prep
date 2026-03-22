"""
# Definition for a Node.
class Node:
    def __init__(self, x: int, next: 'Node' = None, random: 'Node' = None):
        self.val = int(x)
        self.next = next
        self.random = random
"""

class Solution:
    def copyRandomList(self, head: 'Optional[Node]') -> 'Optional[Node]':
        dt={None:None}

        cur=head
        while cur:
            dt[cur]=Node(cur.val)
            cur=cur.next
        
        cur=head
        while cur:
            copyNode=dt[cur]
            copyNode.next=dt[cur.next]
            copyNode.random=dt[cur.random]
            cur=cur.next
        return dt[head]