# Layoffs Data Cleaning & Exploratory Analysis (MySQL)

This project cleans and standardizes a layoffs dataset in MySQL, then runs exploratory analysis to identify trends by company, industry, country, number of staff laid off and time.

## Tools
- MySQL (CTEs, Window Functions)

## Workflow
### Data Cleaning
- Created staging tables to keep raw data untouched
- Identified duplicates using `ROW_NUMBER()` and removed them
- Standardized text fields (company, industry, country)
- Converted `date` from text to `DATE`
- Handled blanks/nulls (converted '' to NULL, populated missing industry using self-join)
- Removed rows with missing layoff metrics
- Dropped helper column (`row_num`)

### Exploratory Analysis
- Max layoffs and full-layoff records
- Layoffs by company, industry, country, year, stage
- Monthly totals and rolling totals
- Top 5 companies per year using `DENSE_RANK()`

## How to Run
1. Load the raw table as (https://github.com/Gogathebrains4/company_layoffs_sql_project/blob/main/layoffs.csv))`
2. Load the SQL file as 

## Author
Wisdom Ogbeche
