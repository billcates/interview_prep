class Solution:
    def maxAreaOfIsland(self, grid: List[List[int]]) -> int:
        res=0

        def dfs(i,j):
            tmp=0
            if i not in range(len(grid)) or j not in range(len(grid[0])) or grid[i][j]==0:
                return tmp
            
            grid[i][j]=0
            tmp+=1
            tmp+=dfs(i+1,j)+dfs(i,j+1)+dfs(i-1,j)+dfs(i,j-1)
            print(i,j,tmp)
            return tmp 
        
        for r in range(len(grid)):
            for c in range(len(grid[0])):
                if grid[r][c]==1:
                    val=dfs(r,c)
                    if val>res:
                        res=val
        return res

#bfs solution

class Solution:
    def maxAreaOfIsland(self, grid: List[List[int]]) -> int:
        res=0

        def bfs(i,j):
            q=collections.deque()
            q.append((i,j))
            tmp=0

            while q:
                i,j=q.popleft()
                if i not in range(len(grid)) or j not in range(len(grid[0])) or grid[i][j]==0:
                    continue
                
                grid[i][j]=0
                tmp+=1
                q.append((i+1,j))
                q.append((i-1,j))
                q.append((i,j+1))
                q.append((i,j-1))
            
            return tmp
        
        for r in range(len(grid)):
            for c in range(len(grid[0])):
                if grid[r][c]==1:
                    val=bfs(r,c)
                    if val>res:
                        res=val
        return res