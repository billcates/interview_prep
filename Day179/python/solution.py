class Solution:
    def isAnagram(self, s: str, t: str) -> bool:
        if len(s)!=len(t):
            return False

        dt={}
        for i in range(len(s)):
            dt[s[i]]=dt.get(s[i],0)+1
            dt[t[i]]=dt.get(t[i],0)-1
        
        for v in dt.values():
            if v!=0:
                return False
        
        return True