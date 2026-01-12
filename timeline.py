import duckdb
import time

con = duckdb.connect()
filename = 'ol_dump_works_2025-12-31.txt.gz'

print("📊 Строим таймлайн человеческих знаний...")
start = time.time()

# SQL ЗАПРОС
# 1. Достаем дату из JSON (created.value)
# 2. Отрезаем от даты первые 4 символа (Год)
# 3. Группируем по году и считаем количество
# 4. Сортируем по году
query = f"""
SELECT 
    SUBSTR(json_extract_string(column4, '$.created.value'), 1, 4) AS Year,
    COUNT(*) as Books_Count
FROM read_csv(
    '{filename}', 
    header=False, 
    delim='\\t', 
    quote='', 
    escape=''
)
GROUP BY Year
ORDER BY Year DESC
"""

df = con.execute(query).df()
end = time.time()

print(f"✅ Готово за {end - start:.4f} сек.")

# Убираем мусор (иногда даты бывают странными, типа "202")
# Оставляем только те, что похожи на реальные годы (числа)
# Это уже pandas-фильтрация результата
df_clean = df[df['Year'].str.isnumeric() == True]

# Показываем топ-10 последних лет
print(df_clean.head(15))

# П.С. Если хочешь сохранить это в файл для Excel:
# df_clean.to_csv("books_by_year.csv", index=False)