class Solution:
    def groupAnagrams(self, strs: List[str]) -> List[List[str]]:
        dt=defaultdict(list)

        for each in strs:
            count=[0]*26

            for every in each:
                count[ord(every)-ord('a')] +=1
            
            dt[tuple(count)].append(each)
        
        return list(dt.values())