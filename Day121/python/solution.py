class Solution:
    def mergeTriplets(self, triplets, target):
        a = b = c = False

        for x, y, z in triplets:
            if x > target[0] or y > target[1] or z > target[2]:
                continue

            if x == target[0]:
                a = True
            if y == target[1]:
                b = True
            if z == target[2]:
                c = True
        return a and b and c
