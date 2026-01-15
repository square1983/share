from metrics import natural_sort_key

test_list = ["C-1", "C-10", "C-2", "C-11", "C-3", "glue_job_1", "glue_job_20", "glue_job_3"]
expected = ["C-1", "C-2", "C-3", "C-10", "C-11", "glue_job_1", "glue_job_3", "glue_job_20"]

sorted_list = sorted(test_list, key=natural_sort_key)
print(f"Original: {test_list}")
print(f"Sorted:   {sorted_list}")

if sorted_list == expected:
    print("SUCCESS: Natural sort works.")
else:
    print("FAILURE: Sort order mismatch.")
    print(f"Expected: {expected}")
