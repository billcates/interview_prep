class Solution:
    def partitionLabels(self, s: str) -> List[int]:
        dt={}

        for i in range(len(s)):
            dt[s[i]]=i
        
        end=0
        start=0
        res=[]
        for i in range(len(s)):
            if dt[s[i]]> end:
                end=dt[s[i]]
            start+=1
            if i==end:
                res.append(start)
                start=0
        return res
