class Solution:
    def isSameTree(self, p: Optional[TreeNode], q: Optional[TreeNode]) -> bool:
        
        # If both are None → same
        if not p and not q:
            return True
        
        # If one is None or values differ → not same
        if not p or not q or p.val != q.val:
            return False
        
        # RETURN the result of both subtree checks
        return self.isSameTree(p.left, q.left) and self.isSameTree(p.right, q.right)
