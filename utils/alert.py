import pandas as pd
import os

def normalize_date_series(s, colname=""):
    """
    Robust date normalization:
    - handles yyyy/m/d
    - handles ISO-8601 with timezone
    - strips time & timezone
    """
    parsed = pd.to_datetime(
        s,
        errors="coerce",
        # infer_datetime_format has been deprecated in newer pandas versions 
        # but matches user's request for robustness across different versions
        infer_datetime_format=True,
        utc=True    # 關鍵：先統一轉成 UTC
    )

    # 去掉時區、只保留日期
    return parsed.dt.tz_localize(None).dt.date

ALERT_DISEASE_MAP = {
    # COVID-19
    "嚴重特殊傳染性肺炎": "COVID-19",
    "新冠併發重症": "COVID-19",

    # Influenza
    "新型A型流感": "新型A型流感",
    "禽類禽流感": "新型A型流感",

    # Mpox
    "M痘": "M痘",

    # Polio
    "小兒麻痺症": "小兒麻痺症",
    "小兒麻痺症/急性無力肢體麻痺": "小兒麻痺症",

    # Vector-borne / others already in color map
    "登革熱": "登革熱",
    "霍亂": "霍亂",
    "屈公病": "屈公病",
    "伊波拉病毒感染": "伊波拉病毒感染",
    "茲卡病毒感染症": "茲卡病毒感染症",
    "麻疹": "麻疹",

    # Everything else → other
    "馬堡病毒出血熱": "馬堡病毒出血熱",
    "拉薩熱": "拉薩熱",
    "黃熱病": "黃熱病",
    "德國麻疹": "德國麻疹",
    "白喉": "白喉",
    "瘧疾": "瘧疾",
    "中東呼吸症候群冠狀病毒感染症": "中東呼吸症候群冠狀病毒感染症",
}



def get_combined_travel_alerts(
    alert_history_path="data/TCDCTravelAlert_history.csv",
    alert_path="data/TCDCTravelAlert.csv"
):
    """
    Modularized function to read, clean, and combine travel alert data.
    """
    # === 1. Read separately ===
    # Use encoding='utf-8-sig' to handle potential Chinese characters/BOM in files
    def read_csv_with_fallback(file_path):
        for enc in ['utf-8-sig', 'cp950']:
            try:
                return pd.read_csv(file_path, encoding=enc)
            except UnicodeDecodeError:
                continue
        # If both fail, try one last resort 'latin1' or raise error
        raise UnicodeDecodeError(f"Unable to read {file_path} with utf-8 or cp950.")
    # === 1. Read separately === 
    df_hist = read_csv_with_fallback(alert_history_path)
    df_curr = read_csv_with_fallback(alert_path)

    df_hist["data_source"] = "TCDCTravelAlert_history"
    df_curr["data_source"] = "TCDCTravelAlert"

    # === 2. Normalize date columns BEFORE concat ===
    for df in [df_hist, df_curr]:
        for col in ["sent", "effective", "expires"]:
            if col in df.columns:
                df[col] = normalize_date_series(df[col], col)

    # === 3. Row bind ===
    df_all = pd.concat(
        [df_hist, df_curr],
        ignore_index=True,
        sort=False
    )

    # === 4. Create analysis date (use effective) ===
    df_all["date"] = df_all["effective"]

    # === 5. Final sanity check ===
    #print(" Combined rows:", len(df_all))
    #print(" effective NaT ratio:", df_all["effective"].isna().mean())

    # 6. disease name mapping
    df_all["alert_disease"] = (
        df_all["alert_disease"]
        .map(ALERT_DISEASE_MAP)
        .fillna("其他疾病")
    )

    return df_all