# ### Timeliness
# - use df_raw (which is transformed back from df) instead of df, i.e. not expanded by disease and country
# - extract end date from description, and get a adjusted source date from SourceTime, SourceTime2, SourceTime_description.
# - calculate interval between source date and publish
# - calculate median value by year and assess missingness
import pandas as pd
import re

def extract_source_time_source(row):
    """
    Extracts date from the 'Source' column using regex.
    """
    source = row['Source']
    date_row = pd.to_datetime(row['date'], errors='coerce')
    year = date_row.year if pd.notna(date_row) else None

    if pd.isna(source) or year is None:
        return pd.NaT

    # Regex to find dates like "9/20" or "8/2"
    matches = re.findall(r'(\d{1,2})/(\d{1,2})', source)
    
    most_recent_date = pd.NaT

    for month_str, day_str in matches:
        month = int(month_str)
        day = int(day_str)
        try:
            date_obj = pd.Timestamp(year=year, month=month, day=day)
            
            # Skip if date_obj is after the reference date
            if date_obj > date_row:
                continue

            # Update if more recent
            if pd.isna(most_recent_date) or date_obj > most_recent_date:
                most_recent_date = date_obj

        except ValueError:
            continue  # Skip invalid dates

    return most_recent_date

def extract_source_time_description(row):
    """
    Extracts the source date from the 'description' column using regex.
    """
    # Only extract if both SourceTime and SourceTime2 are missing
    if pd.notna(row['SourceTime']) or pd.notna(row['SourceTime2']):
        return pd.NaT

    description = row['description']
    date_row = pd.to_datetime(row['date'], errors='coerce')
    
    if pd.isna(date_row):
        return pd.NaT
        
    year = date_row.year

    if pd.isna(description):
        return pd.NaT

    # Find all patterns like "截至今年12/8" or "截至12/8" or "今年截至12/8" or "截至6月25日" or "截至今年6月25日"
    matches = re.findall(r'截至(?:今年)?(\d{1,2})[月/](\d{1,2})日?', description)
    
    if matches:
        dates = []
        for month_str, day_str in matches:
            month = int(month_str)
            day = int(day_str)
            try:
                date_obj = pd.Timestamp(year=year, month=month, day=day)
                dates.append(date_obj)
            except ValueError:
                continue  # skip invalid dates

        if dates:
            max_date = max(dates)
            if max_date > date_row:  # If max_date is later than date_row, treat as missing
                return pd.NaT
            else:
                return max_date    
    return pd.NaT

def calculate_adjusted_source_time(row):
    """
    Determines the best source date based on available columns.
    """
    t1 = row['SourceTime']
    t2 = row['SourceTime2']
    t3 = row['SourceTime_description'] 
    t4 = row['SourceTime_source']
    
    if pd.notna(t1) and pd.notna(t2):
        # Using the most recent source date when both are available
        return t2
    elif pd.notna(t2):
        return t2
    elif pd.notna(t1):
        return t1
    elif pd.notna(t3):
        return t3
    elif pd.notna(t4):
        return t4
    else:
        return pd.NaT

def calculate_interval_source_publish(row):
    """
    Calculates interval between publish date and adjusted source date.
    """
    date_source = pd.to_datetime(row['SourceTime_adj'], errors='coerce')
    date_publish = pd.to_datetime(row['date'], errors='coerce')
    if pd.notna(date_source) and pd.notna(date_publish):
        delta_days = (date_publish - date_source).days
        return delta_days
    else:
        return pd.NaT 

def get_table_timeliness_by_year(df):
    """
    Main function to process the dataframe and return timeliness metrics by year.
    """
    # (0) prepare the df_raw_recovered
    # Dropping duplicates to clean the raw data
    df_raw_recovered = (
        df
        .drop_duplicates(subset=[
            "date",
            "description",
            "Source",
            "SourceTime",
            "SourceTime2"
        ])
        .reset_index(drop=True)
    )

    # (1) extract date from source
    df_raw_recovered['SourceTime_source'] = df_raw_recovered.apply(extract_source_time_source, axis=1)

    # (2) extract the source date from the description 
    # (Internal function name changed slightly to avoid confusion)
    df_raw_recovered['SourceTime_description'] = df_raw_recovered.apply(extract_source_time_description, axis=1)

    # (3) Get adjusted source date
    df_raw_recovered['SourceTime_adj'] = df_raw_recovered.apply(calculate_adjusted_source_time, axis=1)

    # (4) calculate interval between publish date and median source date
    df_raw_recovered['interval_source_publish'] = df_raw_recovered.apply(calculate_interval_source_publish, axis=1)
    df_raw_recovered['year'] = pd.to_datetime(df_raw_recovered['date'], errors='coerce').dt.year

    # (5) Group by year and aggregate
    table_timeliness_byyear = (
        df_raw_recovered.groupby('year')
        .agg(
            median_interval=('interval_source_publish', 'median'),
            mean_interval=('interval_source_publish', 'mean'),
            missing_percent=('SourceTime_adj', lambda x: x.isna().mean().round(3) * 100)
        )
        .reset_index()
    )
    
    return table_timeliness_byyear
