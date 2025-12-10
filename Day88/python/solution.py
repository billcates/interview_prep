class Solution:
    def findKthLargest(self, nums: List[int], k: int) -> int:
        minhp=[]
        i=0
        for each in nums:
            heapq.heappush(minhp,each)
            i+=1
            if i>k:
                heapq.heappop(minhp)
        return minhp[0]
