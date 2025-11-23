import csv

data=[["Name","age"],["a",1],["b",2],["c",3]]

with open("file.csv","w",newline="") as file:
    writer=csv.writer(file)
    writer.writerows(data)

with open("file.csv","r") as f:
    lines=csv.reader(f)
    l=list(lines)

print(l)