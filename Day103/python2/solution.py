class Solution:
    def pacificAtlantic(self, heights: List[List[int]]) -> List[List[int]]:
        row=len(heights)
        col=len(heights[0])

        pac=set()
        atl=set()

        def dfs(i,j,visit,prevh):
            if i<0 or j<0 or i>=row or j>=col or heights[i][j] < prevh or (i,j) in visit:
                return
            
            visit.add((i,j))
            dfs(i+1,j,visit,heights[i][j])
            dfs(i,j+1,visit,heights[i][j])
            dfs(i,j-1,visit,heights[i][j])
            dfs(i-1,j,visit,heights[i][j])

        for c in range(col):
            dfs(0,c, pac, heights[0][c])
            dfs(row-1,c, atl, heights[row-1][c])

        for r in range(row):
            dfs(r,0,pac,heights[r][0])
            dfs(r,col-1,atl,heights[r][col-1])
        
        res=[]
        for i in range(row):
            for j in range(col):
                if (i,j) in pac and (i,j) in atl:
                    res.append((i,j))
        
        return res

        