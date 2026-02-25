class Solution:
    def maxProfit(self, prices: List[int]) -> int:
        _min=prices[0]
        res=0
        for each in prices:
            if each<_min:
                _min=each
            res=max(res,each-_min)
        return res