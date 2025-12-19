class Solution:
    def exist(self, board: List[List[str]], word: str) -> bool:
        row=len(board)
        col=len(board[0])
        path=set()

        def dfs(i,j,pos):
            if pos==len(word):
                return True
            
            if i>=row or j>=col or i<0 or j<0 or word[pos]!=board[i][j] or (i,j) in path:
                return False
                        
            
            path.add((i,j))
            res= (dfs(i,j+1,pos+1) or 
                dfs(i+1,j,pos+1) or 
                dfs(i,j-1,pos+1) or
                dfs(i-1,j,pos+1))
            path.remove((i,j))
            return res

        for r in range(row):
            for c in range(col):
                if dfs(r,c,0):
                    return True
        return False