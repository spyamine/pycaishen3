def chunks(list, n):
    """Yield successive n-sized chunks from l."""

    if n > 1:
        for i in range(0, len(list), n):
            yield list[i:i + n]
    elif n == 1:
        for i in range(0,len(list)):
            yield list[i:i+1]

def print_n(l,chunk_size):
    

    for l in chunks(l,chunk_size):

        print(l)