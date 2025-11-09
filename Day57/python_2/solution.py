class Solution:
    def trap(self, height: List[int]) -> int:
        _trap=0

        if not height:
            return 0

        l = 0
        r = len(height)-1
        max_l =height[0]
        max_r= height[r]
        while l<r:
            print(l,r,max_l,max_r)
            if max_l < max_r:
                l+=1
                max_l = max(max_l, height[l])
                _trap += max_l- height[l]
            else:
                r-=1
                max_r= max(max_r, height[r])
                _trap += max_r - height[r]

        return _trap