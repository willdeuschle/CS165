#/bin/bash
rm db1.bin
rm output.txt
# Milestone 1
echo "Milestone 1"
echo "Test 1 Errors:" > test_results.txt
cat ../project_tests/project_tests_1M/test01.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test01.exp >> test_results.txt
echo "Test 2 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test02.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test02.exp >> test_results.txt
echo "Test 3 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test03.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test03.exp >> test_results.txt
echo "Test 4 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test04.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test04.exp >> test_results.txt
echo "Test 5 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test05.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test05.exp >> test_results.txt
echo "Test 6 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test06.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test06.exp >> test_results.txt
echo "Test 7 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test07.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test07.exp >> test_results.txt
echo "Test 8 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test08.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test08.exp >> test_results.txt
echo "Test 9 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test09.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test09.exp >> test_results.txt
echo "Milestone 2"
echo "Test 10 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test10.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test10.exp >> test_results.txt
echo "Test 11 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test11.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test11.exp >> test_results.txt
echo "Test 12 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test12.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test12.exp >> test_results.txt
echo "Test 13 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test13.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test13.exp >> test_results.txt
echo "Test 14 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test14.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test14.exp >> test_results.txt
echo "Test 15 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test15.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test15.exp >> test_results.txt
echo "Test 16 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test16.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test16.exp >> test_results.txt
echo "Test 17 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test17.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test17.exp >> test_results.txt
echo "Milestone 3"
echo "Test 18 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test18.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test18.exp >> test_results.txt
echo "Test 19 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test19.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test19.exp >> test_results.txt
echo "Test 20 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test20.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test20.exp >> test_results.txt
echo "Test 21 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test21.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test21.exp >> test_results.txt
echo "Test 22 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test22.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test22.exp >> test_results.txt
echo "Test 23 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test23.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test23.exp >> test_results.txt
echo "Test 24 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test24.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test24.exp >> test_results.txt
echo "Test 25 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test25.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test25.exp >> test_results.txt
echo "Test 26 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test26.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test26.exp >> test_results.txt
echo "Test 27 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test27.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test27.exp >> test_results.txt
echo "Test 28 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test28.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test28.exp >> test_results.txt
echo "Test 29 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test29.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test29.exp >> test_results.txt
echo "Milestone 4"
echo "Test 30 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test30.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test30.exp >> test_results.txt
echo "Test 31 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test31.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test31.exp >> test_results.txt
echo "Test 32 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test32.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test32.exp >> test_results.txt
echo "Test 33 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test33.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test33.exp >> test_results.txt
echo "Test 34 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test34.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test34.exp >> test_results.txt
echo "Test 35 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test35.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test35.exp >> test_results.txt
echo "Milestone 5"
echo "Test 36 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test36.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test36.exp >> test_results.txt
echo "Test 37 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test37.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test37.exp >> test_results.txt
echo "Test 38 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test38.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test38.exp >> test_results.txt
echo "Test 39 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test39.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test39.exp >> test_results.txt
echo "Test 40 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test40.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test40.exp >> test_results.txt
echo "Test 41 Errors:" >> test_results.txt
cat ../project_tests/project_tests_1M/test41.dsl | ./client > output.txt && diff output.txt ../project_tests/project_tests_1M/test41.exp >> test_results.txt
echo "DONE, BEFORE SHUTDOWN"
echo "real_shutdown" | ./client
echo "Test Results:"
cat test_results.txt
