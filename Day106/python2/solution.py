class Solution:
    def countComponents(self, n: int, edges: List[List[int]]) -> int:
        adj=[[] for i in range(n)]

        if not edges:
            return n

        for a,b in edges:
            adj[a].append(b)
            adj[b].append(a)

        res=0

        visit=set()
        def dfs(node,par,i):
            if node in visit:
                return 
            visit.add(node)
            if not adj[node]:
                return 
            for nei in adj[node]:
                if nei==par:
                    continue
                dfs(nei,node,i)

        for i in range(n):
            if i not in visit:
                dfs(i,-1,i)
                res+=1
        return res