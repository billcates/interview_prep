class Solution:
    def kClosest(self, points: List[List[int]], k: int) -> List[List[int]]:
        minhp=[]
        for each in points:
            val=each[0]**2 + each[1]**2
            heapq.heappush(minhp,[val,each[0],each[1]])
        
        res=[]
        i=0
        while i<k:
            val=heapq.heappop(minhp)
            res.append([val[1],val[2]])
            i+=1
        return res

# just sorting a 2d array based on the distance should be simpler