from airflow import Dataset


MY_FILE = Dataset("dataset.txt", extra={'test':'test'}) # some path s3://dag1/output_1.txt"

# print(MY_FILE)
# print(MY_FILE.extra)
# print(MY_FILE.uri)