class Solution:
    def lengthOfLongestSubstring(self, s: str) -> int:
        _s=set()
        res=0
        l=0

        for r in range(len(s)):
            while s[r] in _s:
                _s.remove(s[l])
                l+=1
            _s.add(s[r])
            res=max(res,r-l+1)
        return res
