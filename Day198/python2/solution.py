class Solution:
    def solve(self, board: List[List[str]]) -> None:
        """
        Do not return anything, modify board in-place instead.
        """
        r=len(board)
        c=len(board[0])

        def dfs(i,j):
            if i<0 or j<0 or i>=r or j>=c or board[i][j]=='X' or board[i][j]=='#':
                return 
            board[i][j]='#'

            dfs(i+1,j)
            dfs(i-1,j)
            dfs(i,j+1)
            dfs(i,j-1)

        for i in range(c):
            if board[0][i]=='O':
                dfs(0,i)
        
        for i in range(c):
            if board[r-1][i]=='O':
                dfs(r-1,i)

        for i in range(r):
            if board[i][0]=='O':
                dfs(i,0)
        
        for i in range(r):
            if board[i][c-1]=='O':
                dfs(i,c-1)
        
        for i in range(r):
            for j in range(c):
                if board[i][j]=='#':
                    board[i][j]='O'
                elif board[i][j]=='O':
                    board[i][j]='X'