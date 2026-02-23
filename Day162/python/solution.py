class Solution:
    def majorityElement(self, nums: List[int]) -> int:
        dt={}
        for each in nums:
            dt[each]=dt.get(each,0)+1
        _m=-math.inf
        res=nums[0]
        for k,v in dt.items():
            if v>_m:
                _m=v
                res=k
        return res