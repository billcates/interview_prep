class Solution:
    def findTargetSumWays(self, nums: List[int], target: int) -> int:
        l=len(nums)
        dp=[defaultdict(int) for _ in range(l+1)]

        dp[0][0]=1

        for i in range(l):
            for k,v in dp[i].items():
                dp[i+1][k+nums[i]] +=v
                dp[i+1][k-nums[i]] +=v
        
        return dp[l][target]
