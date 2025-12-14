# Definition for singly-linked list.
# class ListNode:
#     def __init__(self, val=0, next=None):
#         self.val = val
#         self.next = next
def getkth(root,k):
    while root and k>0:
        root=root.next
        k-=1
    return root

class Solution:

    def reverseKGroup(self, head: Optional[ListNode], k: int) -> Optional[ListNode]:
        dummy= ListNode(0, head)
        prevgrp=dummy 

        while True:
            kth=getkth(prevgrp, k)
            if not kth:
                break
            groupNext=kth.next

            prev, curr= kth.next, prevgrp.next
            while curr != groupNext:
                nxt=curr.next
                curr.next=prev

                prev=curr
                curr=nxt
            
            tmp=prevgrp.next
            prevgrp.next=kth
            prevgrp=tmp
        return dummy.next