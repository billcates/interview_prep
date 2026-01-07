class Solution:
    def change(self, amount: int, coins: List[int]) -> int:
        n=len(coins)
        coins.sort()
        dp=[[0]*(amount+1) for _ in range(n+1)]

        for i in range(n):
            dp[i][0]=1
        
        for c in range(n-1,-1,-1):
            for a in range(amount+1):
                if a>=coins[c]:
                    dp[c][a]=dp[c+1][a]
                    dp[c][a]+=dp[c][a-coins[c]]
        return dp[0][amount]