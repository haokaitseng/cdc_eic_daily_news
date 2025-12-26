import pandas as pd
import os

def get_processed_visitor_data(file_path='data/表1-2-歷年來臺旅客按居住地分.xlsx', n_top_countries_selected=15):
    """
    Fully modularized function to process visitor data.
    Returns:
        df_visit_by_year_flat: Grouped by year, contains lists of top countries and ISO3s.
        df_visit_flat_all_years: Aggregate over all years, top countries and ISO3s.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    # 1. Read and initial cleaning
    df_visit_raw = pd.read_excel(file_path, header=None, skiprows=2)
    
    header_row1 = df_visit_raw.iloc[0].ffill()
    header_row2 = df_visit_raw.iloc[1].fillna("")
    
    combined_headers = []
    for h1, h2 in zip(header_row1, header_row2):
        h1_str = str(h1).strip()
        h2_str = str(h2).strip()
        if h1_str == h2_str or h2_str == "":
            combined_headers.append(h1_str)
        else:
            combined_headers.append(f"{h1_str}_{h2_str}")
            
    df_visit = df_visit_raw.iloc[2:].copy()
    df_visit.columns = combined_headers
    
    year_col = df_visit.columns[0]
    
    # 2. Reshape to long table
    df_visit_long = df_visit.melt(id_vars=[year_col], var_name='country', value_name='passengers')
    df_visit_long = df_visit_long.rename(columns={year_col: 'year'})
    
    # 3. Data Cleanup
    df_visit_long['passengers'] = pd.to_numeric(
        df_visit_long['passengers'].astype(str).str.replace(',', '').replace('-', '0'), 
        errors='coerce'
    ).fillna(0).astype(int)
    
    # Extract year (last 4 digits)
    df_visit_long = df_visit_long.dropna(subset=['year'])
    df_visit_long['year'] = df_visit_long['year'].astype(str).str[-4:].astype(int)

    # Exclude keywords
    exclude_keywords = '合計|小計|總計'
    df_visit_long_clean = df_visit_long[
        ~df_visit_long['country'].str.contains(exclude_keywords, na=False)
    ].copy()

    # 4. ISO3 Mapping
    country_to_iso3 = {
        '香港.澳門 HongKong. Macao': 'HKG',
        '大陸 Mainland China': 'CHN',
        '日本 Japan': 'JPN',
        '韓國 Korea': 'KOR',
        '印度 India': 'IND',
        '中東 Middle East': None,
        '東南亞地區_馬來西亞 Malaysia': 'MYS',
        '東南亞地區_新加坡 Singapore': 'SGP',
        '東南亞地區_印尼 Indonesia': 'IDN',
        '東南亞地區_菲律賓 Philippines': 'PHL',
        '東南亞地區_泰國 Thailand': 'THA',
        '東南亞地區_越南 Vietnam': 'VNM',
        '東南亞地區_東南亞其他地區 Others': None,
        '亞洲其他地區 Others': None,
        '加拿大 Canada': 'CAN',
        '美國 U.S.A.': 'USA',
        '墨西哥 Mexico': 'MEX',
        '巴西 Brazil': 'BRA',
        '阿根廷 Argentina': 'ARG',
        '美洲其他地區 Others': None,
        '比利時 Belgium': 'BEL',
        '法國 France': 'FRA',
        '德國 Germany': 'DEU',
        '義大利 Italy': 'ITA',
        '荷蘭 Netherlands': 'NLD',
        '瑞士 Switzerland': 'CHE',
        '西班牙 Spain': 'ESP',
        '英國 U.K.': 'GBR',
        '奧地利 Austria': 'AUT',
        '希臘 Greece': 'GRC',
        '瑞典 Sweden': 'SWE',
        '俄羅斯 Russian': 'RUS',
        '歐洲其他地區 Others': None,
        '澳大利亞 Australia': 'AUS',
        '紐西蘭 New Zealand': 'NZL',
        '大洋洲其他地區 Others': None,
        '南非 S. Africa': 'ZAF',
        '非洲其他地區 Others': None,
        '未列明 Unstated': None
    }

    # 5. Process Yearly Data
    df_visit_year_country = (
        df_visit_long_clean
        .groupby(['year', 'country'], as_index=False)['passengers']
        .sum()
    )

    df_by_year_visiting_country = (
        df_visit_year_country
        .sort_values(['year', 'passengers'], ascending=[True, False])
        .groupby('year')
        .head(n_top_countries_selected)
        .reset_index(drop=True)
    )

    df_iso = df_by_year_visiting_country.copy()
    df_iso['iso3'] = df_iso['country'].map(country_to_iso3)

    df_visit_by_year_flat = (
        df_iso
        .sort_values(['year', 'passengers'], ascending=[True, False])
        .groupby('year')
        .agg(
            country=('country', list),
            iso3=('iso3', list)
        )
        .reset_index()
    )

    # 6. Process All-Time Data
    df_visit_all_years_country = (
        df_visit_long_clean
        .groupby('country', as_index=False)['passengers']
        .sum()
    )

    df_visit_top_all_years = (
        df_visit_all_years_country
        .sort_values('passengers', ascending=False)
        .head(n_top_countries_selected)
        .reset_index(drop=True)
    )

    df_visit_top_all_years['iso3'] = df_visit_top_all_years['country'].map(country_to_iso3)

    df_visit_flat_all_years = pd.DataFrame({
        'country': [df_visit_top_all_years['country'].tolist()],
        'iso3': [df_visit_top_all_years['iso3'].tolist()]
    })

    return df_visit_by_year_flat, df_visit_flat_all_years

def clean_visitor_data(file_path='data/表1-2-歷年來臺旅客按居住地分.xlsx'):
    """Legacy wrapper for simple cleaning if needed."""
    # This just returns the full long table for backward compatibility
    df_raw = pd.read_excel(file_path, header=None, skiprows=2)
    header_row1 = df_raw.iloc[0].ffill()
    header_row2 = df_raw.iloc[1].fillna("")
    combined_headers = []
    for h1, h2 in zip(header_row1, header_row2):
        h1_str, h2_str = str(h1).strip(), str(h2).strip()
        combined_headers.append(h1_str if h1_str == h2_str or h2_str == "" else f"{h1_str}_{h2_str}")
    df = df_raw.iloc[2:].copy()
    df.columns = combined_headers
    year_col = df.columns[0]
    df_long = df.melt(id_vars=[year_col], var_name='country', value_name='passengers')
    df_long = df_long.rename(columns={year_col: 'year'})
    df_long['passengers'] = pd.to_numeric(df_long['passengers'].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
    return df_long
