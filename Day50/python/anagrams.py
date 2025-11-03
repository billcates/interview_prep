class Solution:
    def isAnagram(self, s: str, t: str) -> bool:
        a={}
        b={}
        for each in s:
            if each in a:
                a[each]+=1
            else:
                a[each]=1
        for each in t:
            if each in b:
                b[each]+=1
            else:
                b[each]=1
        if len(a)!=len(b):
            return False

        for k,v in a.items():
            if k not in b or a[k]!=b[k]:
                return False
        return True