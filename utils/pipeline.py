import pandas as pd
import unicodedata
from utils.data_loader import (
    load_raw_data, 
    get_transmission_route_mapping, 
    get_who_region_mapping,
    get_source_name_mapping,
    process_source_list
)
from utils.country_name_mapping import (
    build_country_mappings,
    extract_country_iso3_from_description,
    convert_iso2_to_iso3,
    map_headline_country_to_iso3,
    combine_iso_codes
)
from utils.disease_name_mapping import (
    dict_disease_name_mapping,
    dict_disease_name_mapping_en
)
from utils.clean_visitor_data import clean_visitor_data, get_processed_visitor_data

def normalize_token(s):
    """
    Normalizes a token using NFKC normalization and strips whitespace.
    """
    if pd.isna(s):
        return s
    return unicodedata.normalize("NFKC", str(s)).strip()

def run_daily_news_pipeline(epi_xlsx_path, tcdc_csv_path, country_xlsx_path, transmission_xlsx_path, research_end_date='2025-11-27'):
    """
    Orchestrates the entire data processing flow from raw files to the final consolidated DataFrame.
    """
    # 1. Load data
    df_raw, country_mapping_df, dat_transmission_route_raw, region_mapping_df = load_raw_data(
        epi_xlsx_path, tcdc_csv_path, country_xlsx_path, transmission_xlsx_path, research_end_date
    )

    # 2. Country Mapping
    sorted_mapping, headline_cn_to_iso3, iso2_to_iso3 = build_country_mappings(country_mapping_df)
    
    df_raw['description_iso3'] = df_raw['description'].apply(
        lambda x: extract_country_iso3_from_description(x, sorted_mapping)
    )
    df_raw['ISO3166_to_3code'] = df_raw['ISO3166'].apply(
        lambda x: convert_iso2_to_iso3(x, iso2_to_iso3)
    )
    df_raw['headline_country_iso3'] = df_raw['headline_country'].apply(
        lambda x: map_headline_country_to_iso3(x, headline_cn_to_iso3)
    )
    df_raw['country_iso3'] = df_raw.apply(
        lambda row: combine_iso_codes(row['ISO3166_to_3code'], 
                                      row['description_iso3'],
                                      row['headline_country_iso3']),
        axis=1
    )

    # 3. Disease Mapping
    df_raw['disease_name_unlist'] = df_raw['headline_disease'].map(dict_disease_name_mapping).fillna(df_raw['headline_disease'])
    
    # Split into list
    df_raw['disease_name'] = df_raw['disease_name_unlist'].apply(
        lambda x: [d.strip() for d in str(x).split('/')] if pd.notna(x) else None
    )

    # 4. Transmission routes
    route_dict = get_transmission_route_mapping(dat_transmission_route_raw)
    df_raw["transmission_route"] = df_raw["disease_name_unlist"].map(route_dict)

    # 5. Source cleaning
    source_mapping = get_source_name_mapping()
    df_raw['Source_list'] = df_raw['Source'].apply(
        lambda x: process_source_list(x, source_mapping)
    )

    # 6. Consolidation (Explode and Full Names)
    # Selecting meaningful variables as done in original notebook logic
    df_temp = df_raw[["date","country_iso3","disease_name","description","transmission_route","Source","Source_list","SourceTime","SourceTime2"]].copy()
    
    # Explode country
    df_expanded = df_temp.explode('country_iso3').reset_index(drop=True)
    
    # Map back full names
    country_name_map_zh = dict(zip(country_mapping_df['ISO3166-1三位代碼'], country_mapping_df['監測國家/區域']))
    country_name_map_en = dict(zip(country_mapping_df['ISO3166-1三位代碼'], country_mapping_df['監測國家/區域(英文)']))
    
    df_expanded['country_name_zh'] = df_expanded['country_iso3'].map(country_name_map_zh)
    df_expanded['country_name_en'] = df_expanded['country_iso3'].map(country_name_map_en)
    
    # Explode disease
    df = df_expanded.explode('disease_name').reset_index(drop=True)
    
    # Map disease English name
    dict_norm = {normalize_token(k): v for k, v in dict_disease_name_mapping_en.items()}
    df['disease_name_en'] = df['disease_name'].apply(
        lambda tok: dict_norm.get(normalize_token(tok), tok)
    )
    
    # Combined columns
    df['country_disease'] = df.apply(
        lambda row: f"{row['country_name_zh']}_{row['disease_name']}"
        if pd.notna(row['country_name_zh']) and pd.notna(row['disease_name'])
        else None,
        axis=1)
    df['country_disease_en'] = df.apply(
        lambda row: f"{row['country_name_en']}_{row['disease_name_en']}"
        if pd.notna(row['country_name_en']) and pd.notna(row['disease_name_en'])
        else None,
        axis=1)

    # 7. WHO Region Mappings
    region_dict = get_who_region_mapping(region_mapping_df)
    df["WHO_region"] = df["country_iso3"].map(region_dict).fillna("其它")
    
    who_region_map_en = {
        '非洲': 'Africa',
        '美洲': 'Americas',
        '東地中海': 'Eastern Mediterranean',
        '歐洲': 'Europe',
        '東南亞': 'South-East Asia',
        '西太平洋': 'Western Pacific',
        '其它': 'Other'
    }
    df['WHO_region_en'] = df['WHO_region'].map(who_region_map_en).fillna('Other')

    return df

def run_full_pipeline(epi_xlsx_path, tcdc_csv_path, country_xlsx_path, transmission_xlsx_path, visitor_xlsx_path):
    """
    Runs both the daily news pipeline and the visitor data cleaning.
    """
    df_news = run_daily_news_pipeline(epi_xlsx_path, tcdc_csv_path, country_xlsx_path, transmission_xlsx_path)
    df_visitors = clean_visitor_data(visitor_xlsx_path)
    
    return df_news, df_visitors
