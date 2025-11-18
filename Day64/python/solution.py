# 0 1 2 3 4 5 6 7 8 9 10 11 12
#                     a -> 1
#                 b - >1
# c -> 12
#           d -> 7
#       e - >4

# 10 8 5 3 0
# 2  4 1 3 1

# 1  1 7 4 12

# 1
# 1

# 7
# 4

# 12

class Solution:
    def carFleet(self, target: int, position: List[int], speed: List[int]) -> int:
        cars=[]
        for i in range(len(position)):
            cars.append([position[i],((target - position[i])/speed[i])] )

        cars.sort(reverse=True)
        st=[]
        print(cars)
        for pos, time in cars:
            if  not st or time > st[-1]:
                st.append(time)
        return len(st)
