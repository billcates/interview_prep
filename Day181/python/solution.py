class Solution:
    def containsNearbyDuplicate(self, nums: List[int], k: int) -> bool:
        dt={}

        for i,each in enumerate(nums):
            if each in dt:
                if abs(dt[each]-i)<=k:
                    return True
            dt[each]=i
        return False