class Solution:
    def longestConsecutive(self, nums: List[int]) -> int:
        _max=0
        st=set(nums)

        for each in st:
            if each - 1 not in st:
                tmp = 1
                i = each+1
                while i in st:
                    print(each, i)
                    tmp+=1
                    i+=1
                
                if tmp > _max:
                    _max = tmp

        return _max
        