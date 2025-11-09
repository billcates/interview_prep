class Solution:
    def maxArea(self, height: List[int]) -> int:
        i=0
        j=len(height)-1
        _max=0

        while i <j and j>0 and i< len(height):
            x_axis = j-i
            y_axis = min(height[i], height[j])
            tmp_area = x_axis * y_axis

            _max = max(_max, tmp_area)

            if height[i]<=height[j]:
                i+=1
            else:
                j-=1
        
        return _max
        