import pandas as pd
import os

def get_cleaned_press_data(file_path='data/新聞稿_20251229.xlsx', research_end_date = '2025-11-27'):
    """
    Reads news data from Excel, cleans the 'PublishTime' as date only,
    removes HTML tags from 'Content', drops the 'Name' column,
    and removes duplicate entries.
    
    Args:
        file_path (str): Path to the Excel file.
        
    Returns:
        pd.DataFrame: The cleaned dataframe.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    # 1. Read the data
    df_press = pd.read_excel(file_path)

    # 2. Clean 'PublishTime' as date only
    df_press['PublishTime'] = pd.to_datetime(df_press['PublishTime']).dt.date

    # 3. Remove HTML tags from 'Content'
    # Use regex to find tags like <p>, <br />, etc.
    if 'Content' in df_press.columns:
        df_press['Content'] = df_press['Content'].str.replace(r'<[^>]+>', '', regex=True).str.strip()

    # 4. Remove the 'Name' column and drop duplicates
    # Expansion effect is removed by looking at core columns
    cols_to_drop = [c for c in ['Name'] if c in df_press.columns]
    
    # Identify unique rows based on the news metadata and content
    subset_cols = [c for c in ['PublishTime', 'Subject', 'Content'] if c in df_press.columns]
    df_press_cleaned = df_press.drop(columns=cols_to_drop).drop_duplicates(subset=subset_cols)

    end_date = pd.to_datetime(research_end_date).date()
    df_press_cleaned = df_press_cleaned[df_press_cleaned['PublishTime'] <= end_date]


    return df_press_cleaned
