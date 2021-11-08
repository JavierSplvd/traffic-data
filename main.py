import os
from supabase import create_client, Client
from dotenv import load_dotenv
import pandas

load_dotenv()
url: str = os.environ.get('SUPABASE_URL')
key: str = os.environ.get('SUPABASE_KEY')
supabase: Client = create_client(url, key)

# 1 read the csv file
file_path = '01-2021.csv'
csv = pandas.read_csv(file_path, sep=';')
print(csv.head())

# 2 map the csv rows to an object
n = 1000  # chunk row size
list_df = [csv[i:i+n] for i in range(0, csv.shape[0], n)]
for chunk in list_df:
    print("chunk")
    allReadings = []
    for index, row in chunk.iterrows():
        allReadings.append(
            {
                'external_id': row['id'],
                'date': row['fecha'],
                'type': row['tipo_elem'],
                'intensity': row['intensidad'],
                'occupation': row['ocupacion'],
                'load': row['carga'],
                'average': row['vmed'],
                'error': row['error'],
                'interval': row['periodo_integracion'],
            })
    print("chunk on memory")
    # 3 multiple insert
    data = supabase.table('intensities').insert(allReadings).execute()
    print(data)
