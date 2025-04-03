# Country-GDP ETL Project

This project performs ETL (Extract, Transform, Load) operations on data related to the largest banks in the world, based on GDP. The data is scraped from a Wikipedia page, transformed using exchange rates, and loaded into both a CSV file and a SQLite database for further analysis.

## Project Structure

- `main.py` — Python script containing all ETL logic.  
- `exchange_rate.csv` — Required CSV file containing currency exchange rates.  
- `Largest_banks_data.csv` — Output CSV containing the extracted and transformed data.  
- `Banks.db` — SQLite database where the transformed data is loaded.  
- `code_log.txt` — Automatically generated log file tracking script progress.  

## Requirements

- Python 3.x  
- Required Libraries:
  - `beautifulsoup4`
  - `pandas`
  - `numpy`
  - `requests`
  - `lxml`

Install the required libraries with:

```bash
pip install beautifulsoup4 pandas numpy requests lxml
```

Download the required exchange rate file:

```bash
wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv
```

## How to Run

1. Ensure `exchange_rate.csv` is in the same directory as the script.  
2. Run the script using Python:

```bash
python main.py
```

This will:
- Extract the largest banks data from a snapshot of the Wikipedia page.
- Convert market capitalization from USD to GBP, EUR, and INR using exchange rates.
- Save the transformed data to a CSV file.
- Load the data into a SQLite database.
- Run SQL queries and print results to the console.

## Sample Queries Run

```sql
SELECT * FROM Largest_banks;
SELECT AVG(MC_GBP_Billion) FROM Largest_banks;
SELECT `Bank name` FROM Largest_banks LIMIT 5;
```

## Output Files

- `largest_banks_transformed.csv`: CSV with transformed data.  
- `Banks.db`: SQLite database containing the `Largest_banks` table.  
- `code_log.txt`: Log file that records the ETL process steps and timestamps.

## ✍Author

This script was developed as part of the Coursera IBM Data Engineer 16 Course path, by Firat Demirbulakli.
