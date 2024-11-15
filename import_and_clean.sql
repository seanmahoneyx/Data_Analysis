-- SQL Project
-- Data cleaning and preparation for analysis


-- 1. Import data (https://www.kaggle.com/datasets/swaptr/layoffs-2022)
DROP SCHEMA IF EXISTS world_layoffs CASCADE;
CREATE SCHEMA world_layoffs;
CREATE TABLE world_layoffs.layoffs(
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
COPY world_layoffs.layoffs
FROM '/Users/sean/Desktop/DataAnalysis/csvs/layoffs.csv'
DELIMITER ','
HEADER
csv;

SELECT *
FROM world_layoffs.layoffs
LIMIT 10

-- 2. 