#!/bin/python3

import math
import os
import random
import re
import sys

vally1 = []
# Complete the countingValleys function below.
def countingValleys(vally):
    for key,value in enumerate(vally):
        if value=='U':
            vally[key]=1
        else:
            vally[key]=-1

    twoval = 0
    marker = []
    for key,value in enumerate(vally):
        
        twoval = twoval+value
        if twoval ==0:
            marker.append(key)  
    print(vally)
    print(marker)
    twoval = 0
    for key,value in enumerate(marker):
        if key==0:
            if vally[key]==1:
                twoval=0
            else:
                twoval +=1
        else:
            if vally[marker[key-1]+1]==-1:
                twoval= twoval+1
        print('vally'+str(twoval))  
    return twoval
def makeList(s):
    for i in range(0,len(s)):
        vally1.append(s[i])
    return vally1        
if __name__ == '__main__':
    fptr = open(os.environ['OUTPUT_PATH'], 'w')

    n = int(input())

    s = input()
    ls = makeList(s)
    print(ls)
   
    result = countingValleys(ls)
    fptr.write(str(result) + '\n')

    fptr.close()
