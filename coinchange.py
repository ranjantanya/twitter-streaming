def num_ways(n):
    if n==0:
        return 1
    num1,num2,num3=0,0,0
    if n-1>=0:
        num1 = num_ways(n-1)
    if n-2>=0:
        num2 = num_ways(n-2)
    if n-3>=0:
        num3 = num_ways(n-3)
    return num1+num2+num3


if __name__=='__main__':
    print(num_ways(3))
