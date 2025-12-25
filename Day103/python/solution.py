class Solution:
    def orangesRotting(self, grid: List[List[int]]) -> int:
        row=len(grid)
        col=len(grid[0])
        fres=set()
        visit=set()
        q=deque()

        def add(i,j):
            if i<0 or j<0 or i>=row or j>=col or grid[i][j]==0 or (i,j) in visit:
                return
            
            q.append((i,j))
            visit.add((i,j))
            if (i,j) in fres:
                fres.remove((i,j))
            

        for i in range(row):
            for j in range(col):
                if grid[i][j]==1:
                    fres.add((i,j))
                if grid[i][j]==2:
                    visit.add((i,j))
                    q.append((i,j))
        
        ct=0
        if not fres:
            return 0

        while q:

            for _ in range(len(q)):
                i,j=q.popleft()
                print(i,j,ct)

                add(i+1,j)
                add(i,j+1)
                add(i-1,j)
                add(i,j-1)

            ct+=1
        
        return ct-1 if not fres else -1