# Definition for singly-linked list.
# class ListNode:
#     def __init__(self, val=0, next=None):
#         self.val = val
#         self.next = next
class Solution:
    def rotateRight(self, head: Optional[ListNode], n: int) -> Optional[ListNode]:
        if not head:
            return None
        
        cur=head
        ct=0
        while cur:
            ct+=1
            cur=cur.next
        k=n%ct
        if k==0:
            return head

        till=ct-k
        i=1
        tmp=head
        while i<till:
            i+=1
            tmp=tmp.next
        
        last_node=tmp
        new_head=tmp.next
        while tmp.next:
            tmp=tmp.next
        tmp.next=head
        last_node.next=None

        return new_head

# optimization

# Definition for singly-linked list.
# class ListNode:
#     def __init__(self, val=0, next=None):
#         self.val = val
#         self.next = next
class Solution:
    def rotateRight(self, head: Optional[ListNode], n: int) -> Optional[ListNode]:
        if not head:
            return None
        
        cur=head
        prev=None
        ct=1
        while cur:
            ct+=1
            prev=cur
            cur=cur.next
        k=n%ct
        if k==0:
            return head

        till=ct-k
        i=1
        tmp=head
        while i<till:
            i+=1
            tmp=tmp.next
        
        last_node=tmp
        new_head=tmp.next
        prev.next=head
        last_node.next=None

        return new_head