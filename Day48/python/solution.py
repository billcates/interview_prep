# Definition for singly-linked list.
# class ListNode:
#     def __init__(self, val=0, next=None):
#         self.val = val
#         self.next = next
class Solution:
    def modifiedList(self, nums: List[int], head: Optional[ListNode]) -> Optional[ListNode]:

        num_set=set(nums)
        while head.val in num_set:
            head=head.next
        
        tmp=head
        prev=head
        
        while tmp!=None:
            if tmp.val in num_set:
                prev.next=tmp.next
            else:
                prev=tmp
            tmp=tmp.next
            
        return head