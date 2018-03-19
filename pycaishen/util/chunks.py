def chunks(list, n):
    """Yield successive n-sized chunks from l."""

    if n > 1:
        for i in range(0, len(list), n):
            yield list[i:i + n]
    elif n == 1:
        for i in range(0,len(list)):
            yield list[i:i+1]



if __name__ == '__main__':

    l = [1,2,3,4,5]
    n=3

    for l in chunks(l,n):
        print(l)

