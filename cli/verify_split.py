import pandas as pd
import sys

try:
    xl = pd.ExcelFile(sys.argv[1])
    df_ecs = pd.read_excel(xl, sheet_name='ECS')
    print("ECS Columns:", df_ecs.columns)
    
    expected_cols_fragments = ['CPU_Avg_TimeSeries', 'CPU_Max_TimeSeries', 'Mem_Avg_TimeSeries', 'Mem_Max_TimeSeries']
    
    has_all = True
    for col_frag in expected_cols_fragments:
        if not any(col_frag in c for c in df_ecs.columns):
            print(f"MISSING column fragment: {col_frag}")
            has_all = False
            
    if has_all:
        print("SUCCESS: All split series columns found.")
    else:
        print("FAILURE: Columns mismatch.")
        
except Exception as e:
    print(f"Error: {e}")
