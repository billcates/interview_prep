class Solution:
    def convert(self, s: str, numRows: int) -> str:
        l=len(s)
        col=0
        lt = [['_'] * l for _ in range(numRows)]
        ct=0
        while ct<l:
            for row in range(0,numRows):
                if ct==l:
                    break
                lt[row][col]=s[ct]
                ct+=1
            for row in range(numRows-2,0,-1):
                if ct==l:
                    break
                col+=1
                lt[row][col]=s[ct]
                ct+=1
            col+=1
        res=''
        for i in range(numRows):
            for j in range(l):
                if lt[i][j]!='_':
                    res+=lt[i][j]
        return res

# optimized version
class Solution:
    def convert(self, s: str, numRows: int) -> str:
        if len(s)<=numRows or numRows==1:
            return s

        rows=[""]*numRows
        cur_row=0
        direction=1

        for char in s:
            rows[cur_row]+=char

            if cur_row==0:
                direction=1
            if cur_row==numRows-1:
                direction=-1
            cur_row+=direction
        return "".join(rows)
