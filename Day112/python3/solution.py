class Solution:
    def canPartition(self, nums: List[int]) -> bool:
        l=len(nums)
        _s=sum(nums)
        half_sum=_s//2
        if _s % 2>0:
            return False

        memo={}

        def dfs(i,cursum):
            if cursum ==half_sum:
                return True

            if i>=l or cursum > half_sum:
                return False

            if (i,cursum) in memo:
                return memo[(i,cursum)]
            
            memo[(i, cursum)] = dfs(i+1, cursum+nums[i]) or dfs(i+1, cursum)
            return memo[(i, cursum)]

        return dfs(0,0)
