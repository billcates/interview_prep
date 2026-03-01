class Solution:
    def trimTrailingVowels(self, s: str) -> str:
        vowels={'a','e','i','o','u'}

        l=len(s)
        val=-1
        for i in range(l-1,-1,-1):
            if s[i] not in vowels:
                val=i
                break
                
        return s[:val+1]