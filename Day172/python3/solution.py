class Solution:
    def maxArea(self, height: List[int]) -> int:
        i=0
        j=len(height)-1
        res=0
        while i<j:
            if height[i]>height[j]:
                val=(j-i)*(height[j])
                j-=1
            else:
                val=(j-i)*(height[i])
                i+=1
            res=max(val,res)
            # print(res)
        return res