# Definition for singly-linked list.
# class ListNode:
#     def __init__(self, val=0, next=None):
#         self.val = val
#         self.next = next
def mergelinkedlist(list1, list2):
    _res=head= ListNode()
    while list1 and list2:
        if list1.val<list2.val:
            _res.next=list1
            list1=list1.next
        else:
            _res.next=list2
            list2=list2.next
        _res=_res.next
    _res.next= list1 or list2
    print("merged")
    printvalue(_res.next)
    return head.next

def printvalue(root):
    tmp=root
    while tmp:
        print(tmp.val)
        tmp=tmp.next

class Solution:    
    def mergeKLists(self, lists: List[Optional[ListNode]]) -> Optional[ListNode]:
        if not lists:
            return None
        for i in range(1, len(lists)):
            lists[i]=mergelinkedlist(lists[i],lists[i-1])
            
        return lists[-1]