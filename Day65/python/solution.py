class Solution:
    def largestRectangleArea(self, heights: List[int]) -> int:

        st = [] # pos, val
        _max=0

        for i,height in enumerate(heights):
            start=i
            while st and st[-1][1] > height:
                ind,val=st.pop()
                _max=max(_max, val * (i - ind))
                start=ind

            st.append([start,height])
        
        for id,h in st:
            _max = max(_max, h * (len(heights)- id))
        
        return _max

        