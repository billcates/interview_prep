class Solution:
    def findMin(self, nums: List[int]) -> int:
        l=0
        r=len(nums) -1

        while l < r:
            mid= (l+r)//2
            print(l,mid,r, nums[l],nums,nums[r])

            if nums[mid]> nums[r]:
                l=mid+1
            else:
                r=mid
        
        return nums[l]