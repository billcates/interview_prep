class Solution:
    def longestConsecutive(self, nums: List[int]) -> int:
        st=set(nums)
        _max=0

        for each in st:
            if each-1 not in st:
                l=1

                while each+l in st:
                    l+=1

                if l>_max:
                    _max=l
        return _max