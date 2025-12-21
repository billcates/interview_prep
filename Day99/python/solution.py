class Solution:
    def solveNQueens(self, n: int) -> List[List[str]]:
        res=[]

        board=[['.']* n for i in range(n)]
        col=set()
        diag=set()
        ndiag=set()

        def dfs(r):
            if r==n:
                cp=[''.join(i) for i in board]
                res.append(cp)
                return 
            
            for c in range(n):
                if c in col or (r+c) in diag or (r-c) in ndiag:
                    continue
                
                col.add(c)
                diag.add(r+c)
                ndiag.add(r-c)
                board[r][c]='Q'

                dfs(r+1)

                col.remove(c)
                diag.remove(r+c)
                ndiag.remove(r-c)
                board[r][c]='.'
            
        dfs(0)
        return res