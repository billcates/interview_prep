# Definition for a binary tree node.
# class TreeNode:
#     def __init__(self, val=0, left=None, right=None):
#         self.val = val
#         self.left = left
#         self.right = right
class Solution:
    def levelOrder(self, root: Optional[TreeNode]) -> List[List[int]]:
        q=[]
        res=[]
        level=0
        
        if root:
            q.append(root)
        while q:
            # print(q)
            l=len(q)
            res.append([])
            for _ in range(l):
                node=q.pop(0)
                res[level].append(node.val)

                if node.left:
                    q.append(node.left)
                
                if node.right:
                    q.append(node.right)
            level+=1        
        return res

        