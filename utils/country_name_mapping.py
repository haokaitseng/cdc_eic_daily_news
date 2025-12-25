import pandas as pd
import re

def build_country_mappings(country_mapping_df):
    """
    Builds variation_to_iso3 and headline_cn_to_iso3 mappings from the country_mapping_df.
    """
    variation_to_iso3 = {}
    headline_cn_to_iso3 = {}
    iso2_to_iso3 = {}

    for idx, row in country_mapping_df.iterrows():
        iso3 = row['ISO3166-1三位代碼']
        if pd.isna(iso3):
            continue
        
        iso3 = str(iso3).strip()
        
        # Build variations mapping
        variations = set()
        if pd.notna(row['監測國家/區域']):
            variations.add(str(row['監測國家/區域']).strip())
        if 'ISO3166-1(中文)' in row and pd.notna(row['ISO3166-1(中文)']):
            variations.add(str(row['ISO3166-1(中文)']).strip())
        if '外網國家別' in row and pd.notna(row['外網國家別']):
            variations.add(str(row['外網國家別']).strip())
        if '中文別稱' in row and pd.notna(row['中文別稱']):
            aliases = str(row['中文別稱']).split('|')
            variations.update([alias.strip() for alias in aliases])
            
            # Specifically for headline mapping from aliases
            for alias in [a.strip() for a in aliases]:
                headline_cn_to_iso3[alias] = iso3

        for var in variations:
            variation_to_iso3[var] = iso3
            
        # Build ISO2 mapping
        iso2 = row['ISO3166-1二位代碼']
        if pd.notna(iso2):
            iso2_to_iso3[str(iso2).strip()] = iso3

    # Sort variation_to_iso3 by length (descending)
    sorted_variation_mapping = sorted(
        variation_to_iso3.items(),
        key=lambda x: len(x[0]),
        reverse=True
    )

    return sorted_variation_mapping, headline_cn_to_iso3, iso2_to_iso3

def extract_country_iso3_from_description(text, sorted_mapping):
    """
    Extracts ISO3 codes from description text using sorted variation mapping.
    """
    found_iso3 = set()
    if not isinstance(text, str):
        return None

    text_remaining = text

    # Regex pattern to detect "CountryA 公布 CountryB", where the exclusion condition applies (Country A ≠ Country B)
    pattern_pub = re.compile(r'(\S+?)公布(\S+)')
    match = pattern_pub.search(text_remaining)

    excluded_country = None
    included_country = None
    if match:
        excluded_country = match.group(1).strip() 
        included_country = match.group(2).strip() 

    for var, iso3 in sorted_mapping:
        if var in text_remaining:
            if excluded_country and included_country and var == excluded_country and var != included_country:
                continue 

            found_iso3.add(iso3)
            # Remove the matched variation from text_remaining to prevent further substring matches
            text_remaining = text_remaining.replace(var, '')

    return list(found_iso3) if found_iso3 else None

def convert_iso2_to_iso3(iso2_str, mapping_dict):
    """
    Converts a comma-separated string of ISO2 codes to a list of ISO3 codes.
    """
    if pd.isna(iso2_str):
        return None
    
    iso2_str = str(iso2_str).strip()
    iso2_list = [code.strip() for code in iso2_str.split(',')]
    iso3_list = [mapping_dict.get(code) for code in iso2_list if mapping_dict.get(code)]
    
    return iso3_list if iso3_list else None

def map_headline_country_to_iso3(headline_country_str, mapping_dict):
    """
    Maps headline country string (possibly separated by /) to a list of ISO3 codes.
    """
    if pd.isna(headline_country_str):
        return None
    countries = [c.strip() for c in str(headline_country_str).split('/')]
    iso3_list = [mapping_dict.get(c) for c in countries if mapping_dict.get(c)]
    return iso3_list if iso3_list else None

def combine_iso_codes(iso2_3code, description, headline):
    """
    Combines various ISO3 code lists into a single unique list.
    """
    combined_set = set()    
    if isinstance(iso2_3code, list):
        combined_set.update(iso2_3code)
    elif pd.notna(iso2_3code): 
        combined_set.add(iso2_3code)
    
    if isinstance(description, list):
        combined_set.update(description)
    
    if isinstance(headline, list):
        combined_set.update(headline)
    
    return list(combined_set) if combined_set else None
