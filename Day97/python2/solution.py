class Solution:
    def pal(self,s):
        if s==s[::-1]:
            return True
        return False

    def partition(self, s: str) -> List[List[str]]:
        subset=[]
        res=[]

        def dfs(i):
            if i==len(s):
                res.append(subset.copy())  
                return      

            for j in range(i,len(s)):
                part=s[i:j+1]
                if self.pal(part):
                    subset.append(part)
                    dfs(j+1)
                    subset.pop()
        
        dfs(0)
        return res