# Definition for singly-linked list.
# class ListNode:
#     def __init__(self, val=0, next=None):
#         self.val = val
#         self.next = next
class Solution:
    def removeNthFromEnd(self, head: Optional[ListNode], n: int) -> Optional[ListNode]:
        slow=head
        fast=head
        ct=0
        while fast:
            if ct>n:
                slow=slow.next
            ct+=1
            fast=fast.next

        if ct==n:
            return head.next
        
        slow.next=slow.next.next
        return head