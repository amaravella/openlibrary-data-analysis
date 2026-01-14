import pandas as pd
import matplotlib.pyplot as plt # Plotting library

print("🧹 Starting the data cleanup process...")

# 1. Read the result
df = pd.read_csv('final_timeline.csv')

# 2. Convert the year column to numbers
df['year'] = pd.to_numeric(df['year'], errors='coerce')

# 3. Common Sense Filter
# Filtering books from the industrial era (1850) to the current day (2025/2026)
# This removes typos like year '0' or '2999'
clean_df = df[ (df['year'] >= 1850) & (df['year'] <= 2025) ].copy()

# 4. Sort by year (from past to present)
clean_df = clean_df.sort_values('year')

print("\n--- TOP 10 MOST PRODUCTIVE YEARS ---")
# Sort by number of books to see the record holders
print(clean_df.sort_values('books_count', ascending=False).head(10))

print("\n--- GENERATING PLOT ---")
# Drawing a simple graph directly in the code
plt.figure(figsize=(12, 6))
plt.plot(clean_df['year'], clean_df['books_count'], color='blue', linewidth=2)

plt.title('Human History Through Books (Open Library Data)', fontsize=16)
plt.xlabel('Year', fontsize=12)
plt.ylabel('Number of Books Published', fontsize=12)
plt.grid(True, alpha=0.3)

# Save the image
plt.savefig('history_chart.png')
print("🖼️ Chart saved to 'history_chart.png'. Check it out!")