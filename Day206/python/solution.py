class Solution:
    def combinationSum(self, candidates: List[int], target: int) -> List[List[int]]:
        res=[]
        subset=[]
        def backtrack(i):
            if sum(subset)==target:
                res.append(subset.copy())
                return
            if sum(subset)>target or i>len(candidates):
                return
            
            for j in range(i,len(candidates)):
                subset.append(candidates[j])
                backtrack(j)
                subset.pop()

        backtrack(0)
        return res

        