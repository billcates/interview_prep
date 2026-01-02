class Solution:
    def maxProduct(self, nums: List[int]) -> int:
        l=len(nums)
        cur_min=1
        cur_max=1
        res=nums[0]

        for each in nums:
            if each ==0:
                cur_min=1
                cur_max=1

            val=each*cur_max    
            cur_max=max(each, each * cur_max, each * cur_min)
            cur_min=min(each, val, each * cur_min)
            #print(each, val, each * cur_min)
            if res<cur_max:
                res=cur_max
        return res
        