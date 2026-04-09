class Solution:
    def letterCombinations(self, digits: str) -> List[str]:
        st = {
            '2': ['a', 'b', 'c'],
            '3': ['d', 'e', 'f'],
            '4': ['g', 'h', 'i'],
            '5': ['j', 'k', 'l'],
            '6': ['m', 'n', 'o'],
            '7': ['p', 'q', 'r', 's'],
            '8': ['t', 'u', 'v'],
            '9': ['w', 'x', 'y', 'z']
        }
        res=[]
        subset=[]

        def backtrack(level):
            if level==len(digits):
                res.append(''.join(subset.copy()))
                return 
            
            for each in st[digits[level]]:
                subset.append(each)
                backtrack(level+1)
                subset.pop()
        
        backtrack(0)
        return res