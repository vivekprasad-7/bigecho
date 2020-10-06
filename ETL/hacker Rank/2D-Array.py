#!/bin/python3

import math
import os
import random
import re
import sys
import itertools
# Complete the hourglassSum function below.
def hourglassSum(arr):
    firstLoop = 0
    secondLoop = 3
    loopTill = len(arr)
    secondMetrix = []
    sumInner = 0
    sumOuter = 0
    for i in range(0,4):
        secondMetrix.append(list())
        secondMetrix[i] = [0]
        for j in range(0,3):
            secondMetrix[i].append(0) 

    shift1  = 0
    shift2 = 3
    middlels = 1
    for i in range(0,4):
        for j in range(0,4):
            for k in range(shift1,shift2):
                if(k==shift1+1):
                    sumInner+=arr[middlels][k]
                
                elif (k==shift1):
                    sumInner+=arr[i][shift1]
                    sumInner+=arr[i][shift1+1]
                    sumInner+=arr[i][shift1+2]
                elif (k==shift1+2):
                    sumInner+=arr[i+2][shift1]
                    sumInner+=arr[i+2][shift1+1]
                    sumInner+=arr[i+2][shift1+2]    
                print(sumInner,end=" ")    
            sumOuter+= sumInner 
            shift1+=1
            shift2+=1
            sumInner = 0
            secondMetrix[i][j]=sumOuter
            sumOuter = 0
        shift2 = 3 
        shift1 = 0
        middlels += 1
        sumOuter = 0
        sumInner = 0    
    merged = list(itertools.chain(*secondMetrix))    
    return max(merged)    

if __name__ == '__main__':
    fptr = open(os.environ['OUTPUT_PATH'], 'w')

    arr = []

    for _ in range(6):
        arr.append(list(map(int, input().rstrip().split())))

    result = hourglassSum(arr)

    fptr.write(str(result) + '\n')

    fptr.close()
