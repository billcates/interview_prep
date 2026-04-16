class Solution:
    def maxSubarraySumCircular(self, nums: List[int]) -> int:
        _max=maxsum=nums[0]
        _min=minsum=nums[0]

        total=sum(nums)

        for i in range(1,len(nums)):
            _max=max(nums[i],_max+nums[i])
            maxsum=max(maxsum,_max)

            _min=min(nums[i],_min+nums[i])
            minsum=min(minsum,_min)
        
        if maxsum<0:
            return maxsum
        return max(maxsum, total-minsum)