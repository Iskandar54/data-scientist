import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://user:password@localhost:5432/dwh')

orders = pd.read_csv('orders.csv')
items = pd.read_csv('items.csv')

orders['load_date'] = pd.to_datetime('now')
items['load_date'] = pd.to_datetime('now')

orders.to_sql('hub_order', engine, if_exists='append', index=False)
items.to_sql('sat_order', engine, if_exists='append', index=False)

hub_order = pd.read_sql('SELECT * FROM hub_order', engine)
sat_order = pd.read_sql('SELECT * FROM sat_order', engine)

latest_orders = sat_order.sort_values('load_date', ascending=False).drop_duplicates('order_id', keep='first')

latest_orders.to_sql('current_orders', engine, if_exists='replace', index=False)