import pandas as pd
import stadata
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import time
import datetime
from google.cloud import bigquery
from flask import Flask

timestamp = datetime.datetime.now().strftime("%Y%m%d")

client = stadata.Client('262f8ec7c3d56fe547d7f843a9391a87')

def fetch_and_transform(var_id):
	try:
		data = client.view_dynamictable(
			domain='1507',
			var=var_id,
			th='0,9'
		)

		if data is None or len(data) == 0:
            print(f"[SKIP] var_id {var_id} empty")
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

		year_cols = [col for col in data.columns if str(col).isdigit()]

		# UNPIVOT data
		df_long = data.melt(
			id_vars=['var_id', 'metric', 'title', 'sub_name', 'unit', 'variable', 'kategori'],
			value_vars=year_cols,
			var_name='tahun',
			value_name='value'
		)

		# adjust tahun buat jadi angka
		df_long['tahun'] = df_long['tahun'].astype(int)
		return df_long
	except Exception as e:
		print(f"[ERROR] var_id{var_id}: {e}")
		return None

STOPWORDS = {
	'di',
	'menurut',
	'dan',
	'per',
	'dengan'
}

def generate_metric(title, var_id):
	title = title.lower()
	title = re.sub(r'[^a-z0-9\s]', '', title)

	words = [w for w in title.split() if w not in STOPWORDS]

	metric = "_".join(words[:6])
	return f"{metric}_{var_id}"

client.list_dynamictable(all=False, domain=['1507'])
df_meta = client.list_dynamictable(all=False, domain=['1507'])
df_meta['metric'] = df_meta.apply(
    lambda row: generate_metric(row['title'], row['var_id']),
    axis=1
)
meta_map = df_meta.set_index('var_id')[['title', 'sub_name', 'unit', 'metric']].to_dict('index')
var_ids = list(meta_map.keys())

results = []

MAX_WORKERS = 5

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
	futures = {executor.submit(fetch_and_transform, var_id): var_id for var_id in var_ids}

	for future in as_completed(futures):
		var_id = futures[future]
		try:
			result = future.result()
			if result is not None:
				results.append(result)
		except Exception as e:
			print(f"[FATAL] var_id {var_id}: {e}")
	
df_final = pd.concat(results, ignore_index=True)
df_final.to_csv(f"data_{timestamp}.csv", index=False)