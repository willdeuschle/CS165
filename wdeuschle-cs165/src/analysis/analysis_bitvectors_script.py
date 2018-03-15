import os
import time
if __name__ == '__main__':
    total_time = 0
    num_iterations = 3
    for i in range(num_iterations):
        start = time.time()
        os.system('cat analysis/bitvector_test10.dsl | ./client > output.txt')
        end = time.time()
        total_time += end - start
    print "average time: {}".format(total_time / num_iterations)

    print "range 0, 100"
    for i in range(num_iterations):
        start = time.time()
        os.system('cat analysis/bitvector_test100.dsl | ./client > output.txt')
        end = time.time()
        total_time += end - start
    print "average time: {}".format(total_time / num_iterations)

    print "range 0, 1000"
    for i in range(num_iterations):
        start = time.time()
        os.system('cat analysis/bitvector_test1000.dsl | ./client > output.txt')
        end = time.time()
        total_time += end - start
    print "average time: {}".format(total_time / num_iterations)

    print "range 0, 10000"
    for i in range(num_iterations):
        start = time.time()
        os.system('cat analysis/bitvector_test10000.dsl | ./client > output.txt')
        end = time.time()
        total_time += end - start
    print "average time: {}".format(total_time / num_iterations)
