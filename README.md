# Open Library Data Analysis 📚

## 🚀 Overview
This project analyzes historical book publication trends using the **Open Library** dataset (over 40 million records).

The goal was to build a robust **ETL pipeline** capable of processing massive compressed JSON dumps (20GB+) on a local machine, bypassing RAM limitations through streaming and in-process OLAP.

## ✨ Key Features
* **Big Data Processing:** Utilized **DuckDB** to process data larger than available RAM without crashing.
* **Streaming ETL:** Implemented stream reading for `.gz` archives using SQL on top of raw CSV/JSON.
* **Data Cleaning:** Detected and handled anomalies, including:
  * "Monster" records (up to 8MB per line) containing full text or Base64 images.
  * Invalid dates (e.g., year 9999 or non-Gregorian calendars).
* **Visualization:** Generated a timeline of book publications from 1850 to the present day.

## 🛠 Tech Stack
* **Python 3.12**
* **DuckDB** (High-performance in-process SQL OLAP)
* **Pandas** (Final aggregation)
* **Matplotlib** (Visualization)

## ⚙️ How to Run

1. **Clone the repository:**
   ```bash
   git clone [https://github.com/YOUR_USERNAME/openlibrary-data-analysis.git](https://github.com/YOUR_USERNAME/openlibrary-data-analysis.git)
   cd openlibrary-data-analysis