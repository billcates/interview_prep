# Definition for a binary tree node.
# class TreeNode:
#     def __init__(self, val=0, left=None, right=None):
#         self.val = val
#         self.left = left
#         self.right = right
class Solution:
    def goodNodes(self, root: TreeNode) -> int:
        def dfs(root,_maxval):
            if root is None:
                return 0
            if _maxval<=root.val:
                res=1
            else:
                res=0
            _maxval=max(_maxval,root.val)
            res+=dfs(root.left,_maxval)
            res+=dfs(root.right,_maxval)
            return res
        res=dfs(root,root.val)
        return res
        