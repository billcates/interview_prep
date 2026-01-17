class Solution:
    def eraseOverlapIntervals(self, intervals: List[List[int]]) -> int:
        intervals.sort()
        check=intervals[0]
        res=0
        for i in range(1,len(intervals)):
            if intervals[i][0]>=check[1]:
                check=intervals[i]
                continue
            else:
                res+=1
                check[1]=min(check[1],intervals[i][1])
        return res