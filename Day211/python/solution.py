class Solution:
    def minWindow(self, s: str, t: str) -> str:
        if len(s)<len(t):
            return ""
        
        frequency_map={}

        for each in t:
            frequency_map[each]= frequency_map.get(each,0)+1
        
        need=len(frequency_map)
        have=0
        dt={}
        l=0
        res_len=math.inf
        res=''

        for r in range(len(s)):
            char=s[r]
            dt[char]=dt.get(char,0)+1

            if char in frequency_map and frequency_map[char]==dt[char]:
                have+=1

                while have==need:
                    if res_len>(r-l+1):
                        res=s[l:r+1]
                        res_len=r-l+1

                    dt[s[l]]-=1
                    if s[l] in frequency_map and frequency_map[s[l]]>dt[s[l]]:
                        have-=1

                    l+=1
        
        return res