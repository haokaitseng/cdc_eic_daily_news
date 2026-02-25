import pandas as pd
import os
import numpy as np

def get_cleaned_press_data(file_path='data/新聞稿_20251229_fill_until_20251231.xlsx', research_end_date='2025-12-31'):
    """
    Returns a DataFrame with columns: 'Index', 'PublishTime', 'Subject', 'Content', 'Name_merged', and 'Sampled'.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    # 1. Load and Clean (Standard steps)
    df_press = pd.read_excel(file_path)
    df_press['PublishTime'] = pd.to_datetime(df_press['PublishTime']).dt.date
    df_press['Content'] = df_press['Content'].str.replace(r'<[^>]+>', '', regex=True).str.strip()

    # 2. Filter by date
    end_date = pd.to_datetime(research_end_date).date()
    df_press = df_press[df_press['PublishTime'] <= end_date]

    # 3. Group and Merge names
    df_press_cleaned = (
        df_press.groupby(['PublishTime', 'Subject', 'Content'], as_index=False)['Name']
        .apply(lambda x: '、'.join(x.unique()))
    ).rename(columns={'Name': 'Name_merged'})

    # 4. Add Sequential Index (1, 2, 3...)
    # reset_index makes sure the numbers are continuous after the previous filtering/grouping
    df_press_cleaned = df_press_cleaned.reset_index(drop=True)
    df_press_cleaned['Index'] = df_press_cleaned.index + 1

    # 4-2 date cleaning and heading merge
    df_press_cleaned["PublishTime"] = pd.to_datetime(df_press_cleaned["PublishTime"], errors="coerce")

    df_press_cleaned["year"] = df_press_cleaned["PublishTime"].dt.year

    # 清理文字：把 Subject + Content 合併
    df_press_cleaned["Subject"] = df_press_cleaned["Subject"].fillna("").astype(str).str.strip()
    df_press_cleaned["Content"] = df_press_cleaned["Content"].fillna("").astype(str).str.strip()
    df_press_cleaned["subject_content"] = (df_press_cleaned["Subject"] + "。 " + df_press_cleaned["Content"]).str.strip()

    # 5. Add 'Sampled' column (200 random rows marked 1, others 0)
    # We use random_state=4055 (the seed) to ensure reproducibility
    df_press_cleaned['Sampled'] = 0
    
    # Ensure we don't try to sample more than what exists
    sample_size = min(200, len(df_press_cleaned))
    sampled_indices = df_press_cleaned.sample(n=sample_size, random_state=4055).index
    
    df_press_cleaned.loc[sampled_indices, 'Sampled'] = 1

    # 6. human annotation
    df_press_cleaned['flag_international_outbreak_human'] = np.nan

    # Reorder columns for final output
    cols = ['Index', 'PublishTime', 'Subject', 'Content', 'subject_content', 'Name_merged', 'Sampled','flag_international_outbreak_human']
    df_press_cleaned = df_press_cleaned[cols]

    return df_press_cleaned
