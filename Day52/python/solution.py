from collections import Counter
class Solution:
    def topKFrequent(self, nums: List[int], k: int) -> List[int]:
        ct=Counter(nums)
        st=sorted(ct.items(),key=lambda x:x[1],reverse=True)
        st=[v[0] for k,v in enumerate(st[:k])]
        return st