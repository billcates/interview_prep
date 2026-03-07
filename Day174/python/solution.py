class Solution:
    def minSubArrayLen(self, target: int, nums: List[int]) -> int:
        i=0
        j=0
        l=len(nums)
        prefix_sum=0
        res=math.inf
        while j<l and i<=j:
            prefix_sum+=nums[j]
            while prefix_sum>=target:
                res=min(res,j-i+1)
                prefix_sum-=nums[i]
                i+=1
            j+=1

        return res if res!=math.inf else 0