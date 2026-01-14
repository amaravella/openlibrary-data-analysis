import pandas as pd
import matplotlib.pyplot as plt

print("🧹 Starting data cleaning and visualization...")

# 1. Load the aggregated dataset
df = pd.read_csv('final_timeline.csv')

# 2. Convert year to numeric, coercing errors to NaN
df['year'] = pd.to_numeric(df['year'], errors='coerce')

# 3. Apply sanity check filter (exclude future dates and ancient history)
# We focus on the period from 1850 to 2025
clean_df = df[ (df['year'] >= 1850) & (df['year'] <= 2025) ].copy()

# 4. Sort by year
clean_df = clean_df.sort_values('year')

print("\n--- TOP 10 YEARS BY BOOK COUNT ---")
print(clean_df.sort_values('books_count', ascending=False).head(10))

print("\n--- GENERATING CHART ---")
plt.figure(figsize=(12, 6))
plt.plot(clean_df['year'], clean_df['books_count'], color='blue', linewidth=2)

# English labels
plt.title('World Book Publication History (Open Library)', fontsize=16)
plt.xlabel('Year', fontsize=12)
plt.ylabel('Number of Books Published', fontsize=12)
plt.grid(True, alpha=0.3)

# Formatting large numbers on Y-axis (optional, makes it cleaner)
plt.ticklabel_format(style='plain', axis='y')

plt.savefig('history_chart.png')
print("🖼️ Chart saved to 'history_chart.png'. Check it out!")