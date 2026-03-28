# Definition for a binary tree node.
# class TreeNode:
#     def __init__(self, val=0, left=None, right=None):
#         self.val = val
#         self.left = left
#         self.right = right
# preorder = 3,9,20,15,7
# inorder  = 9,3,15,20,7
class Solution:
    def buildTree(self, preorder: List[int], inorder: List[int]) -> Optional[TreeNode]:
        if not preorder or not inorder:
            return None
        root=TreeNode(preorder[0])
        leftindex=inorder.index(preorder[0])
        root.left=self.buildTree(preorder[1:leftindex+1],inorder[:leftindex])
        root.right=self.buildTree(preorder[leftindex+1:],inorder[leftindex+1:])
        return root
