import pandas as pd
import sys

try:
    xl = pd.ExcelFile(sys.argv[1])
    print(f"Sheet names: {xl.sheet_names}")
    if set(xl.sheet_names) == {'Glue', 'Lambda', 'ECS'}:
        print("SUCCESS: All sheets found.")
    else:
        print("FAILURE: Sheets mismatch.")
except Exception as e:
    print(f"Error: {e}")
