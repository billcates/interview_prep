class Solution:
    def singleNumber(self, nums: List[int]) -> int:
        res=0
        for each in nums:
            res ^=each
        return res