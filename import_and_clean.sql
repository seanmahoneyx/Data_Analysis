-- Drop schema if exists and recreate for isolation
DROP SCHEMA IF EXISTS world_layoffs CASCADE;
CREATE SCHEMA world_layoffs;

-- Create raw table for importing data
CREATE TABLE world_layoffs.layoffs_raw (
    company TEXT,
    location TEXT,
    industry TEXT,
    total_laid_off TEXT,
    percentage_laid_off TEXT,
    date TEXT,
    stage TEXT,
    country TEXT,
    funds_raised_millions TEXT
);

-- Import raw data
COPY world_layoffs.layoffs_raw
FROM '/Users/sean/Desktop/DataAnalysis/csvs/layoffs.csv'
DELIMITER ','
CSV HEADER;

-- Create a staging table to preserve raw data
DROP TABLE IF EXISTS world_layoffs.layoffs_staging;
CREATE TABLE world_layoffs.layoffs_staging AS
SELECT *
FROM world_layoffs.layoffs_raw;

-- Remove duplicates
DELETE FROM world_layoffs.layoffs_staging
USING (
    SELECT ctid
    FROM (
        SELECT ctid, 
               ROW_NUMBER() OVER (
                   PARTITION BY company, location, industry, 
                                total_laid_off, percentage_laid_off, date,
                                stage, country, funds_raised_millions
               ) AS row_num
        FROM world_layoffs.layoffs_staging
    ) AS ranked_rows
    WHERE row_num > 1
) AS duplicates_cte
WHERE world_layoffs.layoffs_staging.ctid = duplicates_cte.ctid;


-- Standardize data

-- Trim whitespace from company names
UPDATE world_layoffs.layoffs_staging
SET company = TRIM(company);

-- Condense similar industry names and replace blanks with NULL
UPDATE world_layoffs.layoffs_staging
SET industry = 'Crypto'
WHERE industry ILIKE 'Crypto%';

UPDATE world_layoffs.layoffs_staging
SET industry = NULL
WHERE industry = '';

-- Join table with itself to fill in null industries from same company with info
WITH fill_industries AS (
	SELECT 
		t1.ctid AS target_id,
		t2.industry AS new_industry
	FROM world_layoffs.layoffs_staging t1
    JOIN world_layoffs.layoffs_staging t2
    	ON t1.company = t2.company
    WHERE t1.industry IS NULL
    	AND t2.industry IS NOT NULL 
)
UPDATE world_layoffs.layoffs_staging
SET industry = fill_industries.new_industry
FROM fill_industries
WHERE world_layoffs.layoffs_staging.ctid = fill_industries.target_id;

-- Normalize country names
UPDATE world_layoffs.layoffs_staging
SET country = TRIM(TRAILING '.' FROM country)
WHERE country ILIKE 'United States%';

-- Clean and format date column
-- Replace literal "NULL" strings with actual NULL
UPDATE world_layoffs.layoffs_staging
SET date = NULL
WHERE date ILIKE 'NULL';

-- Add a new column for cleaned dates
ALTER TABLE world_layoffs.layoffs_staging ADD COLUMN date_clean DATE;

-- Convert valid date strings to DATE type
UPDATE world_layoffs.layoffs_staging
SET date_clean = TO_DATE(date, 'MM/DD/YYYY');

-- Drop old text-based date column
ALTER TABLE world_layoffs.layoffs_staging DROP COLUMN date;
ALTER TABLE world_layoffs.layoffs_staging RENAME COLUMN date_clean TO date;

-- Drop useless data with multiple null columns
DELETE FROM world_layoffs.layoffs_staging
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- View cleaned and prepared data
SELECT *
FROM world_layoffs.layoffs_staging
LIMIT 100;
