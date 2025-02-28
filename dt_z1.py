import pandas as pd

items = pd.DataFrame({
    'item_id': [1, 2, 1, 3, 2, 1, 2],
    'name': ['Ручка гелевая', 'Карандаш 1HH', 'Ручка шариковая', 'Ластик', 'Карандаш 1HH', 'Ручка шариковая', 'Карандаш 1H'],
    'price': [10, 2, 10, 5, 3, 5, 7],
    'update_date': pd.to_datetime(['2020-02-01', '2020-01-01', '2020-03-01', '2020-07-01', '2020-05-01', '2020-05-01', '2020-06-01'])
})

orders = pd.DataFrame({
    'order_id': [1, 2, 3, 4, 5, 6],
    'user_id': [1, 2, 1, 3, 2, 1],
    'item_id': [1, 2, 3, 2, 1, 1],
    'order_date': pd.to_datetime(['2020-02-01', '2020-02-01', '2020-07-01', '2020-07-01', '2020-04-01', '2020-06-01'])
})

# 1. Актуальное состояние товаров на 2020-06-01
filtered_items = items[items['update_date'] <= '2020-06-01']
latest_items = filtered_items.sort_values('update_date', ascending=False).drop_duplicates('item_id', keep='first')
print("Актуальное состояние товаров на 2020-06-01:")
print(latest_items)

# 2. Товары, купленные по цене больше или равно чем 3
merged_data = pd.merge(orders, items, on='item_id')
filtered_data = merged_data[merged_data['price'] >= 3]
print("\nТовары, купленные по цене >= 3:")
print(filtered_data)

# 3. Сумма покупок клиента 1
client_orders = merged_data[merged_data['user_id'] == 1]
total_spent = client_orders['price'].sum()
print(f"\nСумма покупок клиента 1: {total_spent}")

# 4. Сумма всех покупок до 2020-05-01 включительно
filtered_orders = merged_data[merged_data['order_date'] <= '2020-05-01']
total_spent = filtered_orders['price'].sum()
print(f"\nСумма всех покупок до 2020-05-01: {total_spent}")

# 5. Сумма всех заказов и средняя цена заказа поквартально
merged_data['quarter'] = merged_data['order_date'].dt.to_period('Q')
quarterly_stats = merged_data.groupby('quarter').agg(
    total_spent=('price', 'sum'),
    avg_price=('price', 'mean')
).reset_index()
print("\nСумма и средняя цена заказа поквартально:")
print(quarterly_stats)

#6. Оптимизация для больших объемов данных
''' Использование Dask
import dask.dataframe as dd
df = dd.read_csv('large_file.csv')
result = df.groupby('column').mean().compute()
'''
''' Использовать PostgreSQL и выполнять запросы напрямую через SQL
'''
