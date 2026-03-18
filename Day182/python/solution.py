class Solution:
    def insert(self, intervals: List[List[int]], newInterval: List[int]) -> List[List[int]]:
        if not intervals:
            return [newInterval]

        res=[]
        start,end=newInterval[0],newInterval[1]

        for i,each in enumerate(intervals):
            if each[1]<start:
                res.append(each)
            elif end<each[0]:
                res.append([start,end])
                res.extend(intervals[i:])
                start,end=-1,-1
                break
            else:
                start=min(start,each[0])
                end=max(end,each[1])

        if start!=-1 and end!=-1:
            res.append([start,end])
        return res