class Solution:
    def canJump(self, nums: List[int]) -> bool:
        _max_reach=nums[0]
        l=len(nums)

        for i in range(len(nums)):
            if i>_max_reach:
                return False
            
            _max_reach=max(_max_reach,nums[i]+i)
            if _max_reach>=l-1:
                return True
        
        return True