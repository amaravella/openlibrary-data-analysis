import duckdb
import time

con = duckdb.connect()

# Убедись, что имена файлов верные!
file_works = 'ol_dump_works_2025-12-31.txt.gz' 
file_editions = 'ol_dump_editions_2025-12-31.txt.gz' # <-- Проверь имя файла editions!

print("🚀 Попытка №3: Увеличиваем лимит размера строки (ловим Годзиллу)...")
start = time.time()

# 1. ТАБЛИЦА ИЗДАНИЙ
# ДОБАВЛЕНО: max_line_size=10000000 (10 МБ)
# DuckDB теперь сможет прочитать даже очень длинные строки
editions_query = f"""
SELECT 
    json_extract_string(column4, '$.works[0].key') AS work_key,
    regexp_extract(json_extract_string(column4, '$.publish_date'), '(\\d{{4}})') AS year
FROM read_csv(
    '{file_editions}', 
    header=False, 
    delim='\\t', 
    quote='', 
    escape='', 
    all_varchar=True, 
    max_line_size=10000000
)
WHERE work_key IS NOT NULL AND year IS NOT NULL
"""

# 2. ТАБЛИЦА ТРУДОВ
# Здесь тоже добавим на всякий случай
works_query = f"""
SELECT 
    column2 AS work_key, 
    json_extract_string(column4, '$.title') AS title
FROM read_csv(
    '{file_works}', 
    header=False, 
    delim='\\t', 
    quote='', 
    escape='', 
    all_varchar=True,
    max_line_size=10000000
)
"""

# 3. JOIN
final_query = f"""
WITH 
    raw_editions AS ({editions_query}),
    raw_works AS ({works_query})

SELECT 
    e.year,
    COUNT(*) as books_count
FROM raw_editions e
JOIN raw_works w ON e.work_key = w.work_key
GROUP BY e.year
ORDER BY e.year DESC
"""

try:
    df = con.execute(final_query).df()
    end = time.time()

    print(f"✅ ФИНАЛЬНАЯ ПОБЕДА! Обработано за {end - start:.4f} сек.")
    print(df.head(20))
    
    # Сохраняем результат
    df.to_csv("final_timeline.csv", index=False)
    print("📄 Файл final_timeline.csv успешно сохранен. Можно открывать в Excel!")

except Exception as e:
    print("❌ Ошибка:", e)