# Definition for a binary tree node.
# class TreeNode:
#     def __init__(self, val=0, left=None, right=None):
#         self.val = val
#         self.left = left
#         self.right = right
class Solution:
    def getleft(self,root):
        tmp=root
        level=0
        while tmp:
            level+=1
            tmp=tmp.left
        return level
    
    def getright(self,root):
        tmp=root
        level=0
        while tmp:
            level+=1
            tmp=tmp.right
        return level

    def countNodes(self, root: Optional[TreeNode]) -> int:
        if not root:
            return 0
        
        left = self.getleft(root)
        right = self.getright(root)

        if left==right:
            return 2 ** left -1
        return self.countNodes(root.left)+self.countNodes(root.right)+1