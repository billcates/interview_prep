class Solution:
    def jump(self, nums: List[int]) -> int:
        
        start=0
        end=0
        step=0
        while True:
            maxi=-math.inf
            for i in range(start,end+1):
                maxi=max(maxi,i+nums[i])
                if i>=len(nums)-1:
                    return step 
            start, end=end+1, maxi
            step+=1
        return step