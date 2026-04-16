class Solution:
    def maxSubArray(self, nums: List[int]) -> int:
        _m=-math.inf
        s=0
        for each in nums:
            s+=each
            if s>_m:
                _m=s
            if s<0:
                s=0
        return _m
        