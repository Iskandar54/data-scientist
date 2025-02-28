# 1. Архитектура DWH на основе Data Vault 2.0 будет состоять из 3 основных таблиц
# 1.1. Хаб для заказов
CREATE TABLE hub_order (
    order_id BIGINT PRIMARY KEY,
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

# 1.2. Спутник для заказов 
CREATE TABLE sat_order (
    order_id BIGINT,
    status VARCHAR(50),
    total_amount DECIMAL(10, 2),
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    hash_diff VARCHAR(64), 
    PRIMARY KEY (order_id, load_date),
    FOREIGN KEY (order_id) REFERENCES hub_order(order_id)
);

# 1.3. Линки для связей
CREATE TABLE link_order_customer (
    order_id BIGINT,
    customer_id BIGINT,
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id, customer_id),
    FOREIGN KEY (order_id) REFERENCES hub_order(order_id),
    FOREIGN KEY (customer_id) REFERENCES hub_customer(customer_id)
);

# 2. Сущности и их роли
# 2.1. Хаб (hub_order):
Роль: Хранение уникальных идентификаторов заказов.
Использование: Быстрый поиск заказов по order_id.

# 2.2. Спутник (sat_order):
Роль: Хранение атрибутов заказов и их истории изменений.
Использование: Отслеживание изменений статуса, стоимости и других атрибутов.

# 2.3. Линки (link_order_customer, link_order_product):
Роль: Описание связей между заказами, клиентами и товарами.
Использование: Анализ связей (например, какие товары чаще всего заказывают определенные клиенты).

# 3. Обновление актуального состояния заказов без updated_at
# 3.1. Хэш-разница
INSERT INTO sat_order (order_id, status, total_amount, hash_diff)
SELECT
    order_id,
    status,
    total_amount,
    MD5(CONCAT(status, total_amount)) AS hash_diff
FROM staging_orders
WHERE MD5(CONCAT(status, total_amount)) NOT IN (
    SELECT hash_diff FROM sat_order WHERE order_id = staging_orders.order_id
);
# 3.2. Сравнение атрибутов:
Сравнивайте каждый атрибут новой записи с последней записью в спутнике.
Если есть изменения, добавляйте новую запись.

# 4. Оптимизация работы с данными
# 4.1. Партиционирование таблиц:
Разделяйте таблицы (например, sat_order) по дате загрузки (load_date) для ускорения запросов.
CREATE TABLE sat_order (
    ...
) PARTITION BY RANGE (load_date);

# 4.2. Индексация:
Создавайте индексы на часто используемых столбцах (например, order_id, load_date).

# 4.3. Компрессия данных:
Используйте сжатие для больших таблиц (например, в PostgreSQL с помощью pg_compression).

