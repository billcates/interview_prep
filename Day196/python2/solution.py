# Definition for a binary tree node.
# class TreeNode:
#     def __init__(self, val=0, left=None, right=None):
#         self.val = val
#         self.left = left
#         self.right = right
from collections import deque
class Solution:
    def rightSideView(self, root: Optional[TreeNode]) -> List[int]:
        if not root:
            return []
        res=[]
        q=deque()

        q.append(root)
        i=0
        while q:
            for _ in range(len(q)):
                tmp=q.popleft()
                if len(res)==i:
                    res.append(tmp.val)

                if tmp.right:
                    q.append(tmp.right)
                if tmp.left:
                    q.append(tmp.left)
            i+=1
        return res
        