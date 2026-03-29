import heapq
class EventManager:

    def __init__(self, events: list[list[int]]):
        self.heap=[(-y,x) for x,y in events]
        heapq.heapify(self.heap)
        self.dt={x:y for x,y in events}

    def updatePriority(self, eventId: int, newPriority: int) -> None:
        self.dt[eventId]=newPriority
        heapq.heappush(self.heap,(-newPriority,eventId))
        

    def pollHighest(self) -> int:
        while self.heap:
            y,x=heapq.heappop(self.heap)
            y=-y
            if self.dt.get(x)==y:
                del self.dt[x]
                return x
        return -1
        
        


# Your EventManager object will be instantiated and called as such:
# obj = EventManager(events)
# obj.updatePriority(eventId,newPriority)
# param_2 = obj.pollHighest()©leetcode