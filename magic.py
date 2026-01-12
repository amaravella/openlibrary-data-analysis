import duckdb
import time

# Подключаемся
con = duckdb.connect()
filename = 'ol_dump_works_2025-12-31.txt.gz'

print("🔮 Начинаю магию превращения JSON в таблицу...")
start = time.time()

# МАГИЧЕСКИЙ ЗАПРОС
# Мы говорим: "Возьми 4-ю колонку (где лежит JSON).
# Найди там поле 'title'. Назови колонку Title.
# Найди там поле 'created' -> внутри него 'value'. Назови Created."
query = f"""
SELECT count(*) as Total_Books
FROM read_csv(
    '{filename}', 
    header=False, 
    delim='\\t', 
    quote='', 
    escape=''
) 
"""

# Выполняем
df = con.execute(query).df()
end = time.time()

print(f"✅ Обработано за {end - start:.4f} сек.")
print("\nВот твоя первая НАСТОЯЩАЯ таблица:")
print(df)