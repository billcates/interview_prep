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
        subset=[]
        res=[]

        def dfs(i):
            if i==len(digits):
                res.append(''.join(subset.copy()))
                return 
            
            for each in st[digits[i]]:
                subset.append(each)
                dfs(i+1)
                subset.pop()
            
        dfs(0)
        return res