class Solution:
    def canConstruct(self, ransomNote: str, magazine: str) -> bool:
        dt_a={}
        dt_b={}

        for each in magazine:
            if each in dt_a:
                dt_a[each]+=1
            else:
                dt_a[each]=1
        
        for each in ransomNote:
            if each in dt_b:
                dt_b[each]+=1
            else:
                dt_b[each]=1
        
        for k,v in dt_b.items():
            if k not in dt_a:
                return False
            if dt_a[k]<v:
                return False
        return True