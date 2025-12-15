class Solution:
    def combinationSum(self, candidates: List[int], target: int) -> List[List[int]]:
        res=[]

        subset=[]
        def dfs(i):
            if sum(subset)==target:
                res.append(subset.copy())
                return 
            
            if sum(subset)> target or i >= len(candidates):
                return 
            
            subset.append(candidates[i])
            dfs(i)

            subset.pop()
            dfs(i+1)
        
        dfs(0)
        return res
