# Definition for a binary tree node.
# class TreeNode:
#     def __init__(self, val=0, left=None, right=None):
#         self.val = val
#         self.left = left
#         self.right = right
class Solution:
    def rightSideView(self, root: Optional[TreeNode]) -> List[int]:
        q=[]
        if root:
            q.append(root)
        res=[]

        while q:
            t=0
            l=len(q)
            for i in range(l):
                node=q.pop(0)
                if i==0:
                    res.append(node.val)
                if node.right:
                    q.append(node.right)
                if node.left:
                    q.append(node.left)
        return res

# dfs approach 

class Solution:
    def rightSideView(self, root: Optional[TreeNode]) -> List[int]:
        res=[]
        def helper(root, level):
            if root is None:
                return
            if level==len(res):
                res.append(root.val)
            helper(root.right,level+1)
            helper(root.left,level+1)
        helper(root,0)

        return res
         