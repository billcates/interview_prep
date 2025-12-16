class Solution:
    def combinationSum2(self, candidates: List[int], target: int) -> List[List[int]]:
        subset=[]
        res=[]
        candidates.sort()
        def dfs(i):
            if sum(subset)==target:
                res.append(subset.copy())
                return
            
            if i>=len(candidates) or sum(subset)>target:
                return 
            
            subset.append(candidates[i])
            dfs(i+1)

            subset.pop()

            while i+1 <len(candidates) and candidates[i]==candidates[i+1]:
                i+=1
            dfs(i+1)

        dfs(0)
        return res
        