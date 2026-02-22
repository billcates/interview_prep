class Solution:
    def removeDuplicates(self, nums: List[int]) -> int:
        i=1
        pos=1
        ct=1

        while i<len(nums):
            if nums[pos-1]==nums[i]:
                if ct<=1:
                    nums[pos]=nums[i]
                    pos+=1
                    ct+=1
            else:
                nums[pos]=nums[i]
                pos+=1
                ct=1
            i+=1
        return pos