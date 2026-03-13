class Solution:
    def isIsomorphic(self, s: str, t: str) -> bool:
        if len(s)!=len(t):
            return False
        dt_1={}
        dt_2={}
        for i in range(len(s)):
            if s[i] in dt_1:
                if dt_1[s[i]] !=t[i]:
                    return False
            else:
                dt_1[s[i]]=t[i]
            if t[i] in dt_2:
                if dt_2[t[i]] !=s[i]:
                    return False
            else:
                dt_2[t[i]]=s[i]
        return True
        