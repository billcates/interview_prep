from collections import defaultdict
class TimeMap:

    def __init__(self):
        self.dt=defaultdict(list)

    def set(self, key: str, value: str, timestamp: int) -> None:
        self.dt[key].append([timestamp, value])  

    def get(self, key: str, timestamp: int) -> str:
        
        if key not in self.dt:
            return ""

        tmp=self.dt[key]
        _res=""
        l=0 
        r=len(tmp)-1
        while l<=r:
            
            mid=(l+r)//2

            if tmp[mid][0] <= timestamp:
                _res=tmp[mid][1]
                l=mid+1
            else:
                r=mid-1

        return _res
        
