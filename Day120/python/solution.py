class Solution:
    def isNStraightHand(self, hand: List[int], groupSize: int) -> bool:
        if len(hand)%groupSize !=0:
            return False

        st={}
        for each in hand:
            if each in st:
                st[each]+=1
            else:
                st[each]=1
        
        minhp=list(st.keys())
        heapq.heapify(minhp)

        while minhp:
            first=minhp[0]
            for i in range(first,first+groupSize):
                if i not in st:
                    return False
                st[i]-=1
                if st[i]==0:
                    if minhp[0]!=i:
                        return False
                    heapq.heappop(minhp)
                    del st[i]
        return True