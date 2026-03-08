class Solution:
    def countnei(self,board,i,j):
        n=0
        rows=[-1,0,1]
        cols=[-1,0,1]
        for a in (rows):
            for b in (cols):
                if a==0 and b==0:
                    continue
                elif (i+a)<0 or (j+b)<0 or (i+a)>= len(board) or (j+b)>=len(board[0]):
                    continue
                elif board[i+a][j+b]in [1,3]:
                    n+=1
        return n


    def gameOfLife(self, board: List[List[int]]) -> None:
        """
        Do not return anything, modify board in-place instead.
        """
        for i in range(len(board)):
            for j in range(len(board[i])):
                nei=self.countnei(board,i,j)
                if board[i][j]==0 and nei==3:
                    board[i][j]=2
                
                if board[i][j]==1:
                    if nei in [2,3]:
                        board[i][j]=3

        for i in range(len(board)):
            for j in range(len(board[i])):
                if board[i][j]==1:
                    board[i][j]=0
                elif board[i][j] in [2,3]:
                    board[i][j]=1