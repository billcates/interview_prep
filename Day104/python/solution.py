class Solution:
    def solve(self, board: List[List[str]]) -> None:
        row=len(board)
        col=len(board[0])

        def dfs(i,j):
            if i<0 or j<0 or i>=row or j>=col or board[i][j]=='X' or board[i][j]=='#':
                return

            board[i][j]='#'
            dfs(i+1,j)
            dfs(i,j+1)
            dfs(i-1,j)
            dfs(i,j-1)

        for c in range(col):
            if board[0][c]=='O':
                dfs(0,c)
            if board[row-1][c]=='O':
                dfs(row-1,c)
        
        for r in range(row):
            if board[r][0]=='O':
                dfs(r,0)
            if board[r][col-1]=='O':
                dfs(r,col-1)

        for r in range(row):
            for c in range(col):
                if board[r][c]=='#':
                    board[r][c]='O'
                elif board[r][c]=='O':
                    board[r][c]='X'
        