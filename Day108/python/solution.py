class Solution:
    def findRedundantConnection(self, edges: List[List[int]]) -> List[int]:
        n=len(edges)
        adj=[[] for _ in range(n+1)]

        def dfs(node,prev):
            if visit[node]:
                return True
            visit[node]=True

            for nei in adj[node]:
                if nei==prev:
                    continue
                if dfs(nei,node):
                    return True
            return False

        for a,b in edges:
            adj[a].append(b)
            adj[b].append(a)
            visit= [False] * (n+1)

            if dfs(a,-1):
                return [a,b]
        return []