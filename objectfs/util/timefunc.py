import time

def timeit(func):
    """Time a function"""    
    def timed(*args, **kwargs):
        try:
            start = time.time()
            return func(*args, **kwrgs)
        finally:
            duration = time.time() - start
            print(duration)
