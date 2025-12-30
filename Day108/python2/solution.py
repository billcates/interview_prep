class Solution:
    def myPow(self, x: float, n: int) -> float:
        
        def power(x, n):
            print(n)
            if n==0:
                return 1
            if x==0:
                return 0
            if n==1:
                return x
            
            tmp=n // 2
            res= power(x, tmp)
            res=res * res
            if n%2:
                res=res *x
            return res

        res=power(x,abs(n))
        if n<0:
            res=1/(res)
        return res