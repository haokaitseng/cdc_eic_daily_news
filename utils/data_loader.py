import pandas as pd
import re

def load_raw_data(epi_xlsx_path, tcdc_csv_path, country_xlsx_path, transmission_xlsx_path, research_end_date='2025-11-27'):
    """
    Loads and performs initial cleaning of the raw data files.
    """
    # 1. READ DATA
    df_source = pd.read_excel(epi_xlsx_path) 
    df_raw = pd.read_csv(tcdc_csv_path)
    country_mapping_df = pd.read_excel(country_xlsx_path)
    dat_transmission_route_raw = pd.read_excel(transmission_xlsx_path)
    region_mapping_df = pd.read_excel(country_xlsx_path, sheet_name="監測國家&區域清單")

    # 2. DATA CLEANING
    df_raw["date"] = pd.to_datetime(df_raw['effective'], errors='coerce').dt.date

    end_date = pd.to_datetime(research_end_date).date()
    # Handle cases where filter date might be different
    df_raw = df_raw[df_raw['date'] <= end_date]

    # Split headline into country and disease
    # regex matches half-width - or full-width － or Box-drawing dash ─
    df_raw[['headline_country', 'headline_disease']] = df_raw['headline'].str.split(r'[-－─]', n=1, expand=True)
    df_raw['headline_country'] = df_raw['headline_country'].str.strip()
    df_raw['headline_disease'] = df_raw['headline_disease'].str.strip()

    # Pre-process df_source
    df_source = df_source[["Subject","Source","SourceTime","SourceTime2","PublishTime"]]
    df_source['SourceTime'] = pd.to_datetime(df_source['SourceTime'], errors='coerce').dt.date
    df_source['SourceTime2'] = pd.to_datetime(df_source['SourceTime2'], errors='coerce').dt.date
    df_source['PublishTime'] = pd.to_datetime(df_source['PublishTime'], errors='coerce').dt.date

    # Merge df_source into df_raw
    df_raw = df_raw.merge(df_source, how="left", left_on=["date","headline"], right_on=["PublishTime","Subject"])

    # drop redundant/useless columns
    drop_cols = ["sent", "effective", "source", "expires", "senderName", "instruction", 
                 "alert_title", "severity_level", "circle", "headline", "PublishTime", "Subject"]
    # Ensure columns exist before dropping
    existing_drop_cols = [c for c in drop_cols if c in df_raw.columns]
    df_raw = df_raw.drop(existing_drop_cols, axis=1)

    return df_raw, country_mapping_df, dat_transmission_route_raw, region_mapping_df

def get_transmission_route_mapping(dat_transmission_route_raw):
    """
    Creates a mapping dictionary for disease transmission routes.
    """
    transmission_lookup = dat_transmission_route_raw[["監測疾病名稱", "主要傳染途徑"]].drop_duplicates()
    route_dict = dict(zip(transmission_lookup["監測疾病名稱"], transmission_lookup["主要傳染途徑"]))
    return route_dict

def get_who_region_mapping(region_mapping_df):
    """
    Creates a mapping dictionary for WHO regions, including manual patches.
    """
    patch_dict = {
        "JEY": "歐洲",     # Jersey
        "REU": "非洲",     # Réunion
        "MYT": "非洲",     # Mayotte
        "GUF": "美洲",     # French Guiana
        "PRI": "美洲",     # Puerto Rico
        "FLK": "美洲",     # Falkland Islands
        "CYM": "美洲",     # Cayman Islands
        "PYF": "西太平洋", # French Polynesia
        "FRO": "歐洲",     # Faroe Islands
        "NCL": "西太平洋", # New Caledonia
        "GIB": "歐洲",     # Gibraltar
        "GUM": "西太平洋", # Guam
        "CXR": "西太平洋", # Christmas Island
        "ASM": "西太平洋", # American Samoa
        "MTQ": "美洲",     # Martinique
        "BMU": "美洲",     # Bermuda
        "WLF": "西太平洋", # Wallis and Futuna
        "ATA": "南極洲",     # Antarctica
        "GLP": "美洲",     # Guadeloupe
        "TKL": "西太平洋", # Tokelau
        "IMN": "歐洲",     # Isle of Man
        "BLM": "美洲",     # Saint Barthélemy
        "MAF": "美洲",     # Saint Martin (French part)
        "VGB": "美洲",     # British Virgin Islands
        "TCA": "美洲",     # Turks and Caicos Islands
        "BES": "美洲",     # Caribbean Netherlands (Bonaire, Sint Eustatius and Saba)
        "ABW": "美洲",     # Aruba
        "CUW": "美洲",     # Curaçao
        "VIR": "美洲",     # U.S. Virgin Islands
        "IOT": "東南亞",     # British Indian Ocean Territory
        "CCK": "西太平洋", # Cocos (Keeling) Islands
        "NFK": "西太平洋", # Norfolk Island
        "GRL": "美洲",     # Greenland
        "SGS": "美洲",     # South Georgia and the South Sandwich Islands
        "SXM": "美洲",     # Sint Maarten (Dutch part)
        "MNP": "西太平洋"  # Northern Mariana Islands
    }
    
    region_dict = region_mapping_df.set_index("ISO3166-1三位代碼")["WHO分區"].to_dict()
    region_dict.update(patch_dict)
    return region_dict

def get_source_name_mapping():
    """
    Returns a mapping dictionary for media source names.
    """
    return {
        'who eis': 'who',
        'cdc': 'us cdc',
        '美國cdc': 'us cdc',
        'who flunet': 'who',    
        'who/afro': 'afro',
        'outbreaknewstoday': 'outbreak news today',
        'global polio eradication initiative' : 'GPEI',
        'WHO Event Information Site for IHR National Focal Point' :'who',
        'uscdc' : 'us cdc',
        'afro' : 'who',
        'wpro' :'who'
    }

def process_source_list(source_str, mapping_dict):
    """
    Processes a raw source string into a cleaned list of sources.
    """
    if pd.isna(source_str):
        return []
    
    # Split by '、' and lowercase
    sources = [s.strip().lower() for s in str(source_str).split('、')]
    
    # Apply mapping
    cleaned_sources = [mapping_dict.get(s, s) for s in sources]
    
    return cleaned_sources
