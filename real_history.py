import duckdb
import time

con = duckdb.connect()
filename = 'ol_dump_works_2025-12-31.txt.gz'

print("📖 Ищем реальные даты публикации книг...")
start = time.time()

# SQL ЗАПРОС
# 1. Берем publish_date (когда издали), а не created (когда добавили в базу).
# 2. regexp_extract(..., '(\d{4})') — ищет 4 цифры подряд (например, 1984 из "c1984").
query = f"""
SELECT 
    regexp_extract(json_extract_string(column4, '$.publish_date'), '(\\d{{4}})') AS Publish_Year,
    COUNT(*) as Books_Count
FROM read_csv(
    '{filename}', 
    header=False, 
    delim='\\t', 
    quote='', 
    escape=''
)
WHERE Publish_Year IS NOT NULL 
  AND Publish_Year > '1500'  -- Отсекаем совсем древность и ошибки
  AND Publish_Year <= '2026' -- Отсекаем будущее
GROUP BY Publish_Year
ORDER BY Publish_Year DESC
"""

df = con.execute(query).df()
end = time.time()

print(f"✅ Готово за {end - start:.4f} сек.")
print(df.head(20))

# Хочешь увидеть график прямо в терминале? (простенький)
# Мы нормализуем данные, чтобы нарисовать звездочки
max_count = df['Books_Count'].max()
print("\n--- ГРАФИК (ASCII ART) ---")
# Берем последние 30 лет для наглядности
subset = df[df['Publish_Year'] >= '1990'].sort_values('Publish_Year')
for index, row in subset.iterrows():
    # Длина палочки зависит от количества книг
    bar_len = int((row['Books_Count'] / max_count) * 50) 
    print(f"{row['Publish_Year']} | {'█' * bar_len} ({row['Books_Count']})")