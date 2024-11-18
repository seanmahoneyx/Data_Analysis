-- SQL Project (PostgreSQL)
-- Data cleaning and preparation for analysis


-- 
-- 1. Import data (https://www.kaggle.com/datasets/swaptr/layoffs-2022)
DROP SCHEMA IF EXISTS world_layoffs CASCADE;
CREATE SCHEMA world_layoffs;
CREATE TABLE world_layoffs.layoffs_raw(
company text,
location text,
industry text,
total_laid_off text,
percentage_laid_off text,
date text,
stage text,
country text,
funds_raised_millions text
);
COPY world_layoffs.layoffs_raw
FROM '/Users/sean/Desktop/DataAnalysis/csvs/layoffs.csv'
DELIMITER ','
HEADER
csv;

SELECT *
FROM world_layoffs.layoffs_raw
LIMIT 10

-- 
-- 2. Create staging table to preserve raw data
DROP TABLE IF EXISTS world_layoffs.layoffs_staging;
CREATE TABLE world_layoffs.layoffs_staging AS
TABLE world_layoffs.layoffs_raw;

SELECT *
FROM world_layoffs.layoffs_staging
LIMIT 10

-- 
-- 3. Remove duplicates
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

-- 
-- 4. Standardize data
	--4a. Remove whitespace from company names
UPDATE world_layoffs.layoffs_staging
SET company = TRIM(company);
	--4b. Condense similar industry names
UPDATE world_layoffs.layoffs_staging
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'
	--4c. Find and correct country names
UPDATE world_layoffs.layoffs_staging
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%'

	--4d. Format date column (I hate regex but I'm learning)
UPDATE world_layoffs.layoffs_staging
SET date = NULL
WHERE NOT date ~ '^\d{2}/\d{2}/\d{4}$';

UPDATE world_layoffs.layoffs_staging
SET date = TO_DATE(date, 'MM/DD/YYYY');

ALTER TABLE world_layoffs.layoffs_staging RENAME COLUMN date TO date_backup;
ALTER TABLE world_layoffs.layoffs_staging ADD COLUMN date DATE;
UPDATE world_layoffs.layoffs_staging
SET date = TO_DATE(date_backup, 'MM/DD/YYYY');
--verify changed to date data type
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'layoffs_staging' AND table_schema = 'world_layoffs' and column_name = 'date';



ALTER TABLE world_layoffs.layoffs_staging DROP COLUMN date_backup;

SELECT *
FROM world_layoffs.layoffs_staging
LIMIT 100
