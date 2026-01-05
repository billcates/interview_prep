class Solution:
    def maxProfit(self, prices: List[int]) -> int:
        
        dp={}

        def dfs(i,buy):
            if i>=len(prices):
                return 0
            
            if (i,buy) in dp:
                return dp[(i,buy)]

            cooldown=dfs(i+1, buy)
            if buy:
                val1=dfs(i+1, not buy) - prices[i]
            else:
                val1=dfs(i+2, not buy) + prices[i]
            dp[(i,buy)]=max(val1,cooldown)
            return dp[(i,buy)]
        
        return dfs(0,True)