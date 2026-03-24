# Definition for singly-linked list.
# class ListNode:
#     def __init__(self, val=0, next=None):
#         self.val = val
#         self.next = next
class Solution:
    def partition(self, head: Optional[ListNode], x: int) -> Optional[ListNode]:
        if head is None:
            return None
        dummy=ListNode()
        d1=dummy

        dummy2=ListNode(0,head)
        d2=dummy2

        cur=head
        while cur:
            if cur.val<x:
                dummy2.next=cur
                dummy2=dummy2.next
            else:
                dummy.next=cur
                dummy=dummy.next
            cur=cur.next

        dummy.next=None
        dummy2.next=d1.next
        return d2.next