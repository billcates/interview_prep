class Solution:
    def merge(self, intervals: List[List[int]]) -> List[List[int]]:
        if not intervals:
            return []

        intervals.sort()
        res = []
        curr_start, curr_end = intervals[0]

        for i in range(1, len(intervals)):
            start, end = intervals[i]

            if start <= curr_end:  # overlap
                curr_end = max(curr_end, end)
            else:
                res.append([curr_start, curr_end])
                curr_start, curr_end = start, end

        res.append([curr_start, curr_end])
        return res