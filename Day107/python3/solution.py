class Solution:
    def plusOne(self, digits: List[int]) -> List[int]:
        carry=1
        res=[]
        for i in range(len(digits)-1,-1,-1):
            if carry>0 and digits[i]==9:
                res.insert(0,0)
                carry=1
            elif carry>0:
                res.insert(0,digits[i]+1)
                carry=0
            else:
                res.insert(0,digits[i])
            print(carry)
        if carry>0:
            res.insert(0,1)
        return res