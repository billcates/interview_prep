class Solution:
    def dailyTemperatures(self, temperatures: List[int]) -> List[int]:
        _res=[0] * len(temperatures)
        stack = [] #[ind,temp]

        for i in range(len(temperatures)):

            while stack and stack[-1][1] < temperatures[i]:
                val=stack[-1][0]
                _res[stack[-1][0]] = i - val
                stack.pop()
            
            stack.append([i,temperatures[i]])
        
        return _res
        