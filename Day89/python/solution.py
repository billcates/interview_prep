class Solution:
    def leastInterval(self, tasks: List[str], n: int) -> int:
        counts=Counter(tasks)
        maxhp=[-cnt for cnt in counts.values()]
        heapq.heapify(maxhp)

        q=deque()
        time=0

        while q or maxhp:
            time+=1

            if maxhp:
                val=1+ heapq.heappop(maxhp)
                if val:
                    q.append([val, time+n])
            
            if q and q[0][1]==time:
                heapq.heappush(maxhp,q.popleft()[0])
        return time


