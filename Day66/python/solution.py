class Solution:
    def search(self, nums: List[int], target: int) -> int:
        l=0
        r=len(nums)-1
               
        while l<=r:
            mid= l+ (r - l)//2
            val= nums[mid]
            print(val, l ,r)
            
            if val == target:
                return mid
            elif val > target:
                r=mid-1
            else:
                l=mid+1
        
        return -1
        