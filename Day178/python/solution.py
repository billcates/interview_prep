class Solution:
    def wordPattern(self, pattern: str, s: str) -> bool:
        s=s.split()
        if len(pattern)!=len(s):
            return False
        dt_1={}
        dt_2={}
        for i in range(len(pattern)):
            if pattern[i] in dt_1:
                if dt_1[pattern[i]]!=s[i]:
                    return False
            else:
                dt_1[pattern[i]]=s[i]
            
            if s[i] in dt_2:
                if dt_2[s[i]]!=pattern[i]:
                    return False
            else:
                dt_2[s[i]]=pattern[i]
        return True
