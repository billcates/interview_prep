matrix =[[1,2,3],[4,5,6],[7,8,9]]

# 1   2   3
# 4   5   6
# 7   8   9

# 3   6   9
# 2   5    8
# 1    4    7

# 1   2   3 |  4
# 5   6   7   8
# 9   10  11  12
# 13  14  15  16

# 13  9   5    1
# 14  10  6    2
# 15  11  7   3
# 16  12  8    4

l=len(matrix)
for i in range(l//2):
    for j in range(i,l-i-1):
        top=matrix[i][j]
        matrix[i][j]=matrix[j][l-1-i]
        matrix[j][l-1-i]=matrix[l-1-i][l-1-j]
        matrix[l-1-i][l-1-j]=matrix[l-1-j][i]
        matrix[l-1-j][i]=top

print(matrix)