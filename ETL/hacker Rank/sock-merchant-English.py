import math
n=9
numbers=[1,1,3,1,2,1,3,3,3,3]
count={}
numbers.sort()
print(numbers)
latest = 0
    
for key,value in enumerate(numbers):
    if key<=len(numbers)-2:
        
        a= numbers[key]
        b= numbers[key+1]
        if a==b:
            latest+=1
            count.update({a:latest})
        if a!=b:
            latest=0
print(count)
pairvalue = 0
for i in count.values():
       
    if i==1:
        pairvalue += i
        print(pairvalue)
    else:
        pairvalue += int(math.ceil(i/2))
        print("Ceil",pairvalue)
print("Total pair of socks is:: ",pairvalue)