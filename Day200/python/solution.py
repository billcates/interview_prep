from collections import defaultdict
class Solution:
    def calcEquation(self, equations: List[List[str]], values: List[float], queries: List[List[str]]) -> List[float]:
        adj=defaultdict(list)
        for i, eq in enumerate(equations):
            src,tgt= eq[0],eq[1]
            adj[src].append([tgt,values[i]])
            adj[tgt].append([src,1/values[i]])
        
        def bfs(src,tgt):
            if src not in adj or tgt not in adj:
                return -1
            q=deque()
            visited=set()
            visited.add(src)
            q.append([src,1])

            while q:
                n, w=q.popleft()
                if n==tgt:
                    return w
                for node, weight in adj[n]:
                    if node not in visited:
                        q.append([node,w*weight])
                        visited.add(node)
            return -1

        return [bfs(each[0],each[1]) for each in queries]