class DetectSquares:

    def __init__(self):
        self.dt={}

    def add(self, point: List[int]) -> None:
        if (point[0],point[1]) in self.dt:
            self.dt[(point[0],point[1])]+=1
        else:
            self.dt[(point[0],point[1])]=1

    def count(self, point: List[int]) -> int:
        x,y=point
        res=0
        for k,v in self.dt.items():
            x2,y2=k
            if abs(x2-x) != abs(y2-y) or (x2==x or y2==y):
                continue
            if (x,y2) in self.dt and (x2,y) in self.dt:
                res+=(v * (self.dt[(x, y2)] * self.dt[(x2, y)]))
        return res
            

        


# Your DetectSquares object will be instantiated and called as such:
# obj = DetectSquares()
# obj.add(point)
# param_2 = obj.count(point)