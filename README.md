## NOTES & ASSUMPTIONS
- I've written some integration tests for parts 1. Due to time constraints, I didn't write unit tests, and I 
didn't write an integration test for Parts 2 and 3, nor for the airflow DAG, though I accept this is not best practice
- For task2_3.py I used column name 'review_scores_value'. This is an assumption.

## SETUP
Developed with spark 3.2.1, Python 3.8.8
pip install -r requirements.txt

## RUNNING

### Part1 (Tasks 2, Task 3)
`spark-submit src/task1.py -f groceries.csv -u out/out_1_2a.txt -c out/out_1_2b.txt -t out/out_1_3.txt` 

### Part2 (Tasks 2)
`spark-submit src/task2_2.py -f sf-airbnb-clean.parquet -o out/out_2_2.txt` 

### Part2 (Tasks 3)
`spark-submit src/task2_3.py -f sf-airbnb-clean.parquet -o out/out_2_3.txt` 

### Part2 (Tasks 4)
`spark-submit src/task2_4.py -f sf-airbnb-clean.parquet -o out/out_2_4.txt` 

### Part2 (Tasks 5)
`python src/task2_5.py ` 

### Part3 
`spark-submit src/task3.py -f /tmp/iris.csv -o out/out_3_2.txt` 


## TESTS
`cd test` 

`pytest`
