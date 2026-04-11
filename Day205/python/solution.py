class Solution:
    def permute(self, nums: List[int]) -> List[List[int]]:
        res=[]
        subset=[]
        def backtrack(i):
            if len(subset)==len(nums):
                res.append(subset.copy())
                return
            for each in nums:
                if each not in subset:
                    subset.append(each)
                    backtrack(i+1)
                    subset.pop()
        backtrack(0)
        return res