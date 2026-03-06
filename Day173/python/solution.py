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


class Solution:
    def subarraySum(self, nums: List[int], k: int) -> int:
        prefix = 0
        dt = {0:1}
        count = 0

        for num in nums:
            prefix += num
            
            if prefix - k in dt:
                count += dt[prefix - k]
            
            if prefix in dt:
                dt[prefix]+=1
            else:
                dt[prefix]=1
        return count
                