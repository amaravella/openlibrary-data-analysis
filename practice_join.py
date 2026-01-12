import duckdb

con = duckdb.connect()

print("👨‍🏫 Тренируем JOIN на котиках...")

# 1. Создаем виртуальную таблицу "Котики" (Authors)
con.execute("""
CREATE TABLE cats (
    cat_id INTEGER,
    name VARCHAR
);
INSERT INTO cats VALUES 
    (1, 'Барсик'),
    (2, 'Мурзик'),
    (3, 'Рыжик'); -- Рыжик ленивый, он ничего не поймал
""")

# 2. Создаем виртуальную таблицу "Пойманные мыши" (Books/Works)
con.execute("""
CREATE TABLE mice (
    mouse_id INTEGER,
    catcher_id INTEGER, -- Это ключ связи! (Кто поймал)
    weight_grams INTEGER
);
INSERT INTO mice VALUES 
    (101, 1, 25),  -- Барсик поймал
    (102, 1, 30),  -- Барсик поймал еще одну
    (103, 2, 15);  -- Мурзик поймал
""")

# 3. ДЕЛАЕМ JOIN!
# Мы хотим увидеть: Имя кота | Вес мыши
# Связь: cats.cat_id = mice.catcher_id
query = """
SELECT 
    cats.name AS Cat_Name,
    mice.weight_grams AS Mouse_Weight
FROM cats
JOIN mice ON cats.cat_id = mice.catcher_id
"""

print("\n--- Результат JOIN (Кто что поймал) ---")
print(con.execute(query).df())

# 4. А теперь ОКОННАЯ ФУНКЦИЯ
# Мы хотим пронумеровать мышей ДЛЯ КАЖДОГО КОТА отдельно.
# Барсик: мышь №1, мышь №2.
# Мурзик: мышь №1.
window_query = """
SELECT 
    cats.name,
    mice.weight_grams,
    ROW_NUMBER() OVER (PARTITION BY cats.name ORDER BY mice.weight_grams DESC) as mouse_rank
FROM cats
JOIN mice ON cats.cat_id = mice.catcher_id
"""

print("\n--- Результат Window Function (Рейтинг уловов) ---")
print(con.execute(window_query).df())