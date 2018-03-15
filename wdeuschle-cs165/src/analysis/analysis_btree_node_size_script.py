import os
import time
if __name__ == '__main__':
    total_time = 0
    num_iterations = 4
    print "analyzing load performance"
    for i in range(num_iterations):
        os.system('make && ./server &')
        os.system('rm db1.bin')
        start = time.time()
        os.system('cat analysis/btree_node_size_test_load.dsl | ./client > output.txt')
        end = time.time()
        if i is not num_iterations - 1:
            os.system("echo 'real_shutdown' | ./client")
        total_time += end - start
    print "average load time: {}".format(total_time / num_iterations)

    execute_total_time = 0
    num_execute_iteratinos = 100
    print "analyzing select, fetch, print perf"
    for i in range(num_execute_iteratinos):
        start = time.time()
        os.system('cat analysis/btree_node_size_test_execute.dsl | ./client > output.txt')
        end = time.time()
        execute_total_time += end - start
    print "average execute time: {}".format(execute_total_time / num_execute_iteratinos)
