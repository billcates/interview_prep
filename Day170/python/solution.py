class Solution:
    def lengthOfLastWord(self, s: str) -> int:
        s=s.split()
        lastword=s[-1]
        return len(lastword)