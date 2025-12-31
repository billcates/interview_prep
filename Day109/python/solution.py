class Solution:
    def minCostClimbingStairs(self, cost: List[int]) -> int:
        if len(cost)<=2:
            return min(cost)
        dp=[0]* (len(cost)+2)
        l=len(cost)
        
        for i in range(l-1,-1,-1):
            dp[i]=cost[i]+min(dp[i+1],dp[i+2])
        return min(dp[0],dp[1])