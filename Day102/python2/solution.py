class Solution:
    def islandsAndTreasure(self, grid: List[List[int]]) -> None:
        row=len(grid)
        col=len(grid[0])
        q=collections.deque()
        visit=set()

        def add(i,j):
            if i<0 or j<0 or i>=row or j >=col or (i,j) in visit or grid[i][j]==-1:
                return
            
            q.append((i,j))
            visit.add((i,j))

        for i in range(row):
            for j in range(col):
                if grid[i][j]==0:
                    q.append((i,j))
                    visit.add((i,j))
        
        dist=0
        while q:
            for _ in range(len(q)):
                i,j=q.popleft()
                grid[i][j]=dist

                add(i+1,j)
                add(i,j+1)
                add(i-1,j)
                add(i,j-1)
            dist+=1