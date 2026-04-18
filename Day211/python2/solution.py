class Solution:
    def checkSubarraySum(self, nums: List[int], k: int) -> bool:
        dt={0:-1}
        prefix_sum=0

        for i,each in enumerate(nums):
            prefix_sum+=each
            rem= prefix_sum % k
            if rem in dt:
                if i - dt[rem]>=2:
                    return True
            else:    
                dt[rem] =i
        return False