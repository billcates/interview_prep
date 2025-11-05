from collections import Counter
class Solution:
    def topKFrequent(self, nums: List[int], k: int) -> List[int]:
        # builder the counter
        ct=Counter(nums)
        #sort based on freq
        st=sorted(ct.items(),key=lambda x:x[1],reverse=True)
        #keep first k elements
        st=[v[0] for k,v in enumerate(st[:k])]
        return st
    

# 2nd approach - bucket sort

from collections import Counter
class Solution:
    def topKFrequent(self, nums: List[int], k: int) -> List[int]:
        #counter
        ct=Counter(nums)
        #buckets of length nums to track frequency
        freq=[[] for i in range(len(nums)+1)]

        #track frequency
        for key,v in ct.items():
            freq[v].append(key)

        res=[]
        # iterate the frequency from reverse till length of res is k
        for i in range(len(freq)-1,0,-1):
            for j in freq[i]:   
                res.append(j)

                if len(res) == k:
                    return res


