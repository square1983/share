import pandas as pd
import sys

try:
    xl = pd.ExcelFile(sys.argv[1])
    print(f"Sheet names: {xl.sheet_names}")
    
    expected_sheets = {'Glue', 'Lambda', 'ECS', 'Glue Detail', 'ECS Detail'}
    if not expected_sheets.issubset(set(xl.sheet_names)):
        print(f"FAILURE: Missing sheets. Found: {xl.sheet_names}")
        sys.exit(1)
        
    df_ecs_det = pd.read_excel(xl, sheet_name='ECS Detail')
    print("\nECS Detail Columns:", df_ecs_det.columns[:10])
    
    # Check for Time_1
    if 'Time_1' in df_ecs_det.columns:
        print("SUCCESS: Found Time_1 column in ECS Detail.")
    else:
        print("FAILURE: Time_1 column missing.")
        
    if 'StepName' in df_ecs_det.columns:
         print("SUCCESS: Found StepName column.")
    else:
         print("FAILURE: StepName column missing.")

except Exception as e:
    print(f"Error: {e}")
