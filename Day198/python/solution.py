class Solution:
    def numIslands(self, grid: List[List[str]]) -> int:
        def dfs(i,j):
            if i>=len(grid) or j>=len(grid[0]) or grid[i][j]!='1' or i<0 or j<0:
                return

            grid[i][j]='Q'
            dfs(i-1,j)
            dfs(i+1,j)
            dfs(i,j-1)
            dfs(i,j+1)

        ct=0
        for i in range(len(grid)):
            for j in range(len(grid[0])):
                if grid[i][j]=='1':
                    dfs(i,j)
                    ct+=1
        
        return ct