class Solution:
    def jump(self, nums: List[int]) -> int:
        start=0
        end=0
        res=0
        l=len(nums)
        _max=0

        while True:
            _max=-math.inf
            for i in range(start,end+1):
                if i >=l-1:
                    return res
                _max=max(_max,i+nums[i])
            start,end= end+1, _max
            res+=1
        return res