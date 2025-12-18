class Solution:
    def generateParenthesis(self, n: int) -> List[str]:
        subset=[]
        res=[]

        def dfs(left,right):
            if left==n and right==n:
                res.append(''.join(subset.copy()))
                return
            
            if left<n:
                subset.append('(')
                dfs(left+1,right)
                subset.pop()
            if right<left:
                subset.append(')')
                dfs(left,right+1)
                subset.pop()
            
        dfs(0,0)
        return res