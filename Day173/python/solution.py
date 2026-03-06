# O(n2)

class Solution:
    def subarraySum(self, nums: List[int], k: int) -> int:
        res=0
        for i in range(len(nums)):
            tmp=nums[i]
            if tmp==k:
                res+=1
            for j in range(i+1,len(nums)):
                tmp+=nums[j]
                if tmp==k:
                    res+=1
                if tmp>k:
                    break
        return res
