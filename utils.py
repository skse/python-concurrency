import subprocess
import sys
import time


def time_it(f):
    def d(*args, **kwargs):
        start = time.time()
        result = f(*args, **kwargs)
        end = time.time()
        print(f'Execution time for {f.__name__} function is: {end - start:.6f} s')
        return result
    return d


def try_open_file_manager(directory):
    try:
        if sys.platform == 'win32':
            subprocess.Popen(['start', directory], shell=True)
        elif sys.platform == 'darwin':
            subprocess.Popen(['open', directory])
        else:
            subprocess.Popen(['xdg-open', directory])
    except Exception:
        print('Failed to open output dir in a file manger, but life goes on')
