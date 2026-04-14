import os
import pandas as pd
import stadata
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import datetime
from google.cloud import bigquery

# timestamp (optional, kalau mau logging)
timestamp = datetime.datetime.now().strftime("%Y%m%d")

# ENV
API_KEY = os.getenv("API_KEY")
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET = os.getenv("DATASET")
PROD_TABLE_NAME = os.getenv("PROD_TABLE")

# FULL TABLE NAME
PROD_TABLE = f"{PROJECT_ID}.{DATASET}.{PROD_TABLE_NAME}"

# CLIENT
client = stadata.Client(API_KEY)
bq_client = bigquery.Client()

STOPWORDS = {
    'di',
    'menurut',
    'dan',
    'per',
    'dengan'
}


def fetch_and_transform(var_id, meta_map):
    try:
        data = client.view_dynamictable(
            domain='1507',
            var=var_id,
            th='0,9'
        )

        if data is None or len(data) == 0:
            print(f"[SKIP] {var_id}")
            return None

        # tambah metadata
        data['var_id'] = var_id
        data['metric'] = meta_map[var_id]['metric']
        data['title'] = meta_map[var_id]['title']
        data['sub_name'] = meta_map[var_id]['sub_name']
        data['unit'] = meta_map[var_id]['unit']

        # rename kolom
        data = data.rename(columns={
            'turunan variable': 'kategori'
        })

        # ambil kolom tahun
        year_cols = [col for col in data.columns if str(col).isdigit()]

        # unpivot
        df_long = data.melt(
            id_vars=[
                'var_id',
                'metric',
                'title',
                'sub_name',
                'unit',
                'variable',
                'kategori'
            ],
            value_vars=year_cols,
            var_name='tahun',
            value_name='value'
        )

        df_long['tahun'] = df_long['tahun'].astype(int)

        return df_long

    except Exception as e:
        print(f"[ERROR] var_id {var_id}: {e}")
        return None


def generate_metric(title, var_id):
    title = title.lower()
    title = re.sub(r'[^a-z0-9\s]', '', title)

    words = [w for w in title.split() if w not in STOPWORDS]
    metric = "_".join(words[:6])

    return f"{metric}_{var_id}"


def enforce_schema(df):
    df['var_id'] = df['var_id'].astype(int)
    df['metric'] = df['metric'].astype(str)
    df['title'] = df['title'].astype(str)
    df['sub_name'] = df['sub_name'].astype(str)
    df['unit'] = df['unit'].astype(str)
    df['variable'] = df['variable'].astype(str)
    df['kategori'] = df['kategori'].astype(str)
    df['tahun'] = df['tahun'].astype(int)
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    return df


def run_pipeline():
    print("Starting pipeline...")

    # ambil metadata
    df_meta = client.list_dynamictable(all=False, domain=['1507'])

    df_meta['metric'] = df_meta.apply(
        lambda row: generate_metric(row['title'], row['var_id']),
        axis=1
    )

    meta_map = df_meta.set_index('var_id')[
        ['title', 'sub_name', 'unit', 'metric']
    ].to_dict('index')

    var_ids = list(meta_map.keys())

    results = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(fetch_and_transform, var_id, meta_map): var_id
            for var_id in var_ids
        }

        for future in as_completed(futures):
            res = future.result()
            if res is not None:
                results.append(res)

    if not results:
        print("No data fetched, skip upload")
        return "No data"

    df_final = pd.concat(results, ignore_index=True)
    df_final = enforce_schema(df_final)

    print(f"Total rows: {len(df_final)}")

    # upload ke BigQuery
    job = bq_client.load_table_from_dataframe(df_final, PROD_TABLE)
    job.result()

    print("Loaded to BigQuery")

    return "Success"


if __name__ == "__main__":
    run_pipeline()
