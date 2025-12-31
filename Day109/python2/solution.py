class Solution:
    def rob(self, nums: List[int]) -> int:
        if not nums:
            return 0
        l=len(nums)

        if l==1:
            return nums[0]
        dp=[0] * (l)
        dp[0]=nums[0]
        dp[1]=max(nums[0],nums[1])
        for i in range(2,l):
            dp[i]=max(dp[i-2]+nums[i],dp[i-1])
        return dp[l-1]