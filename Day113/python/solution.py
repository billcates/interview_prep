class Solution:
    def uniquePaths(self, m: int, n: int) -> int:
        res=0
        memo={}

        def dfs(i,j):
            if i==m-1 and j==n-1:
                return 1

            if i>=m or j>=n:
                return 0

            if (i,j) in memo:
                return memo[]

            left=dfs(i,j+1)
            right=dfs(i+1,j)
            memo[(i,j)]=left+right

            return left+right
        
        res=dfs(0,0)
        return res
  
 