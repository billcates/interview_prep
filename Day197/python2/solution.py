# Definition for a binary tree node.
# class TreeNode:
#     def __init__(self, val=0, left=None, right=None):
#         self.val = val
#         self.left = left
#         self.right = right
class Solution:
    def zigzagLevelOrder(self, root: Optional[TreeNode]) -> List[List[int]]:
        if not root:
            return []
        res=[]
        q=deque()
        q.append(root)

        i=0
        while q:
            tmp=[]
            for _ in range(len(q)):
                node=q.popleft()
                tmp.append(node.val)
                if node.left:
                    q.append(node.left)
                if node.right:
                    q.append(node.right)
            if i%2==0:
                res.append(tmp)
            else:
                res.append(tmp[::-1])
            i+=1
        return res
        