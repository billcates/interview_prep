"""
Definition of Interval:
class Interval(object):
    def __init__(self, start, end):
        self.start = start
        self.end = end
"""

class Solution:
    def minMeetingRooms(self, intervals: List[Interval]) -> bool:
        intervals.sort(key=lambda x:x.start)
        minhp=[]

        for i in intervals:
            if minhp and minhp[0]<=i.start:
                heapq.heappop(minhp)
            heapq.heappush(minhp,i.end)

        return len(minhp)
