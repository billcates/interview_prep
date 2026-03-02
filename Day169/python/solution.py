class Solution:
    def longestCommonPrefix(self, strs: List[str]) -> str:
        if not strs:
            return ""
        
        l = min(len(s) for s in strs)
        prefix = ""
        
        for i in range(l):
            char = strs[0][i]
            
            for j in range(1, len(strs)):
                if strs[j][i] != char:
                    return prefix
            
            prefix += char
        
        return prefix