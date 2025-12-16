class Solution:
    def permute(self, nums: List[int]) -> List[List[int]]:
        subset=[]
        res=[]

        def dfs():
            if len(subset)==len(nums):
                res.append(subset.copy())
                return 
            
            for each in nums:
                if each not in subset:
                    subset.append(each)
                    dfs()
                    subset.pop()
            
        dfs()
        return res