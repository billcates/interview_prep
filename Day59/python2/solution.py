class Solution:
    def checkInclusion(self, s1: str, s2: str) -> bool:
        if len(s1) > len(s2):
            return False
        
        st={}
        for each in s1:
            st[each]=st.get(each,0)+1
        
        l=0
        r=len(s1)
        st1={}
        tmp=s2[:r]
        for each in tmp:
            st1[each]=st1.get(each,0)+1
        
        while r<=len(s2):
            if st == st1:
                return True

            if r<len(s2):
                st1[s2[r]]=st1.get(s2[r],0)+1

            st1[s2[l]]-=1
            if st1[s2[l]] == 0:
                del st1[s2[l]]
                
            l+=1
            r+=1
        
        return False
            

