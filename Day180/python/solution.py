class Solution:
    def isHappy(self, n: int) -> bool:
        s=set()
        tmp=n
        while True:
            tmp_res=0
            while tmp>0:
                val=tmp%10
                tmp=tmp//10
                tmp_res+=val ** 2
                # print(tmp,tmp_res,val)
            if tmp_res==1:
                return True
            tmp=tmp_res
            if tmp in s:
                return False
            s.add(tmp)