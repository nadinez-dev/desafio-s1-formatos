import pandas as pd
df = pd.read_csv('pedidos_ecommerce.csv')
# Volume: quantas linhas e colunas?
print(df.shape)
# Tipos: o que é número, o que é texto, o que é data?
print(df.dtypes)
# Primeiras linhas: como o dado se parece?
df.head(10)

import os
df.to_csv('pedidos.csv', index=False)
df.to_json('pedidos.json', orient='records', lines=True)
df.to_parquet('pedidos.parquet', index=False)
# Veja o tamanho de cada arquivo em disco
# Preste atenção na diferença entre os três
for arquivo in ['pedidos.csv', 'pedidos.json', 'pedidos.parquet']:
    kb = os.path.getsize(arquivo) / 1024
    print(f'{arquivo:30s} {kb:.1f} KB')



import duckdb
import time
queries = {
    'CSV': "SELECT cidade, SUM(valor_final) as total FROM 'pedidos.csv'GROUP BY cidade ORDER BY total DESC",
    'JSON': "SELECT cidade, SUM(valor_final) as total FROM 'pedidos.json'GROUP BY cidade ORDER BY total DESC",
    'Parquet': "SELECT cidade, SUM(valor_final) as total FROM 'pedidos.parquet' GROUP BY cidade ORDER BY total DESC",
}
for nome, query in queries.items():
    inicio = time.time()
    resultado = duckdb.sql(query).df()
    fim = time.time()
    print(f'{nome:10s} {(fim - inicio)*1000:.2f} ms')
# Os três resultados abaixo devem ser idênticos
# O formato não muda o dado — muda como ele é armazenado e lido
print(resultado)