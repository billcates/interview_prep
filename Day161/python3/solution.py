from functools import cmp_to_key

class Solution:
    def largestNumber(self, nums: List[int]) -> str:
        lt=list(map(str,nums))

        def compare(a,b):
            if a+b>b+a:
                return -1
            elif a+b<b+a:
                return 1
            else:
                return 0
        
        lt.sort(key=cmp_to_key(compare))

        if lt[0] == "0":
            return "0"

        return "".join(lt)

        