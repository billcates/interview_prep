# Definition for a binary tree node.
# class TreeNode:
#     def __init__(self, val=0, left=None, right=None):
#         self.val = val
#         self.left = left
#         self.right = right
from collections import deque
class Solution:
    def averageOfLevels(self, root: Optional[TreeNode]) -> List[float]:
        if not root:
            return 0
        q=deque()
        res=[]

        q.append(root)
        while q:
            tmplt=[]
            for _ in range(len(q)):
                node=q.popleft()
                tmplt.append(node.val)
                if node.left:
                    q.append(node.left)
                if node.right:
                    q.append(node.right)
            avg=sum(tmplt)/len(tmplt)
            res.append(avg)
        return res