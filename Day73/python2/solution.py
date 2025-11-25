# Definition for singly-linked list.
# class ListNode:
#     def __init__(self, val=0, next=None):
#         self.val = val
#         self.next = next
class Solution:
    def mergeTwoLists(self, list1: Optional[ListNode], list2: Optional[ListNode]) -> Optional[ListNode]:
        _res=head=ListNode()

        while list1 and list2:
            if list1.val<list2.val:
                _res.next=list1
                list1=list1.next
            else:
                _res.next=list2
                list2=list2.next
            _res=_res.next
        _res.next =list1 or list2
        
        return head.next
    
###################
#recursion

def mergeTwoLists_recursive(list1, list2):
    if not list1:
        return list2
    if not list2:
        return list1
    
    if list1.val < list2.val:
        list1.next = mergeTwoLists_recursive(list1.next, list2)
        return list1
    else:
        list2.next = mergeTwoLists_recursive(list2.next, list1)
        return list2