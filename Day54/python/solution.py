class Solution:
    def productExceptSelf(self, nums: List[int]) -> List[int]:
        l=len(nums)
        pref=[0]*l
        suff=[0]*l
        pref[0] = suff[l-1] = 1

        for i in range(1,l):
            pref[i]=pref[i-1]*nums[i-1]
        
        for i in range(l-2,-1,-1):
            suff[i]=suff[i+1] * nums[i+1]
        
        res=[0]* l
        for ind in range(l):
            res[ind]=pref[ind]*suff[ind]
        
        return res