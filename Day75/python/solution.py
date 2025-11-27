# Definition for singly-linked list.
# class ListNode:
#     def __init__(self, val=0, next=None):
#         self.val = val
#         self.next = next
class Solution:
    def removeNthFromEnd(self, head: Optional[ListNode], n: int) -> Optional[ListNode]:
        tmp=head
        ct=0
        while tmp:
            ct+=1
            tmp=tmp.next
        pos=ct-n


        if pos==0:
            return head.next
        ct=1
        tmp=head

        while ct<=pos:
            if ct==pos:
                next=tmp.next
                next_2=next.next if next else None
                tmp.next=next_2
                return head
            ct+=1
            tmp=tmp.next
        
########################
#two pointers


class Solution:
    def removeNthFromEnd(self, head: Optional[ListNode], n: int) -> Optional[ListNode]:
        dummy=ListNode(0,head)
        left=dummy
        right=head

        while n>0:
            right=right.next
            n-=1
        
        while right:
            left=left.next
            right=right.next
        
        left.next=left.next.next
        return dummy.next