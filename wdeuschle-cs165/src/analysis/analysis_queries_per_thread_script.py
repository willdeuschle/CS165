import os
import time
if __name__ == '__main__':
    total_time = 0
    num_iterations = 100
    for i in range(num_iterations):
        start = time.time()
        os.system('cat analysis/queries_per_thread.dsl | ./client > output.txt')
        end = time.time()
        total_time += end - start
    print "average time: {}".format(total_time / num_iterations)
