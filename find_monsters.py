import duckdb
import time

con = duckdb.connect()
filename = 'ol_dump_editions_2025-12-31.txt.gz' # Проверь имя файла!

print("🦖 Выпускаем кракена... Ищем самые тяжелые записи.")
print("Придется прочитать весь файл, так что подожди минуту.")
start = time.time()

# SQL ЗАПРОС
# 1. length(column4) - считает количество символов.
# 2. Делим на 1024*1024, чтобы получить Мегабайты.
# 3. substr(column4, 1, 500) - берем первые 500 букв, чтобы понять, что там,
#    но не завалить терминал миллионами символов.

query = f"""
SELECT 
    (length(column4) / 1024.0 / 1024.0) AS Size_MB,
    json_extract_string(column4, '$.title') AS Title,
    column1 AS Key,
    substr(column4, 1, 200) AS Preview_Start
FROM read_csv(
    '{filename}', 
    header=False, 
    delim='\\t', 
    quote='', 
    escape='', 
    all_varchar=True, 
    max_line_size=20000000 -- Ставим 20 МБ, чтобы точно влезло всё
) 
ORDER BY length(column4) DESC
LIMIT 5;
"""

df = con.execute(query).df()
end = time.time()

print(f"✅ Найдено за {end - start:.4f} сек.")
print("\n--- ТОП-5 САМЫХ ТЯЖЕЛЫХ КНИГ ---")
print(df)

# Если хочешь увидеть "внутренности" чемпиона полностью:
top_content_query = f"""
SELECT column4 
FROM read_csv('{filename}', header=False, delim='\\t', quote='', escape='', all_varchar=True, max_line_size=20000000) 
ORDER BY length(column4) DESC 
LIMIT 1
"""
print("\n--- ЧТО ВНУТРИ ЧЕМПИОНА (Первые 1000 символов) ---")
champion = con.execute(top_content_query).fetchone()[0]
print(champion[:1000] + "...\n[ОСТАЛЬНОЕ ОБРЕЗАНО]")