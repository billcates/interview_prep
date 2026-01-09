class Solution:
    def maxSubArray(self, nums: List[int]) -> int:
        n=len(nums)
        maxi=-math.inf
        s=0
        for each in nums:
            s+=each
            if s> maxi:
                maxi=s
            if s<0:
                s=0
        return maxi
