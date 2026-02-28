class Solution:
    def productExceptSelf(self, nums: List[int]) -> List[int]:
        m=1
        l=len(nums)
        res=[1]*l
        for i in range(1,l):
            m=m*nums[i-1]
            res[i]=m

        right=1
        for i in range(l-1,-1,-1):
            res[i]*=right
            right=right*nums[i]
        return res