class Solution:
    def findOrder(self, numCourses: int, prerequisites: List[List[int]]) -> List[int]:
        pre={ i: [] for i in range(numCourses)}

        for crs,pr in prerequisites:
            pre[crs].append(pr)
            
        visit=set()
        total_visit=set()
        res=[]
        def dfs(crs):
            if crs in visit:
                return False
            if crs in total_visit:
                return True

            visit.add(crs)
            for each in pre[crs]:
                if not dfs(each):
                    return False

            visit.remove(crs)
            res.append(crs)
            total_visit.add(crs)
            
            return True
        
        for i in range(numCourses):
            if not dfs(i):
                return []
        return res