class Solution:
    def isHappy(self, n: int) -> bool:
        tmp=n
        st=set()
        while tmp:
            s=0
            while tmp:
                s=s+(tmp%10)**2
                tmp=tmp//10
            if s==1:
                return True
            tmp=s
            if tmp in st:
                return False
            st.add(tmp)
        return False

        