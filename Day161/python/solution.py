class Solution:
    def removeDuplicates(self, nums: List[int]) -> int:
        pos=1
        i=1
        while i<len(nums):
            if nums[i]!=nums[pos-1]:
                nums[pos]=nums[i]
                pos+=1
            i+=1
        return pos