class Solution:
    def lastStoneWeight(self, stones: List[int]) -> int:
        max_hp=[-x for x in stones]
        heapq.heapify(max_hp)

        while len(max_hp)>1:
            a=-(heapq.heappop(max_hp))
            b=-(heapq.heappop(max_hp))
            if a-b >0:
                heapq.heappush(max_hp,-(a-b))
            elif b-a >0:
                heapq.heappush(max_hp,-(b-a))
            
        if max_hp:
            return -max_hp[0]
        return 0

        