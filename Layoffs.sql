-- Data Cleaning

SELECT *
FROM layoffs;


CREATE TABLE layoffs_staging                      -- To duplicate data, so you can have the original untouched
LIKE layoffs;

SELECT *
FROM layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *,
ROW_NUMBER() OVER(                                                     -- To check for duplicates  
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage
, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(                                                     -- To check for duplicates  
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage
, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *                           -- Filter out suspected duplicates
FROM duplicate_cte
WHERE row_num > 1;

SELECT *                                	-- To chross check a suspected duplicate
FROM layoffs_staging
WHERE company = 'Casper';

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(                                                     -- To check for duplicates  
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage
, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
DELETE                           -- Delete duplicates (this won't work because you cannot edit a cte)
FROM duplicate_cte
WHERE row_num > 1;

													-- create a blank table
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *                                	-- view table
FROM layoffs_staging2

INSERT INTO layoffs_staging2                -- insert values into new table)
SELECT *,
ROW_NUMBER() OVER(                                                     -- To check for duplicates  
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage
, country, funds_raised_millions) AS row_num
FROM layoffs_staging

SELECT *                                	-- check for duplicates in new table
FROM layoffs_staging2
WHERE row_num > 1;

SET SQL_SAFE_UPDATES = 0; 						-- To avoid that safe mode error

DELETE                                	-- Delete duplicates from new table
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *                                	-- Validate that dupliactes have been deleted
FROM layoffs_staging2;


-- Standardizing data

SELECT company, TRIM(company) 				-- Trim extra spaces in column "company"
FROM layoffs_staging2;

UPDATE layoffs_staging2						-- Update table with trimed column
SET company = TRIM(company);

SELECT DISTINCT industry                    -- Look at column "indusrty" (we saw discrepancies in rows with crypto)
FROM layoffs_staging2
ORDER BY 1;

SELECT *                    				-- Look at column "indusrty" row "crypto" 
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2						-- Update column "industry" turn "cryto currency" to "crypto"
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country,  TRIM(TRAILING '.' FROM country)                  -- Look at column "country"
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2						-- Update column "country" turn "united states." to "united states"
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT `date`,								-- change date to prefered date format
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2						-- Update table
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2				-- Change date data type from string to date
MODIFY COLUMN `date` DATE;


-- Checking for Null values
SELECT *										-- Check for null values in columns
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *										-- Check for null values in columns
FROM layoffs_staging2
WHERE `date` IS NULL;

UPDATE layoffs_staging2                         -- Change '' to NULL
SET industry = NULL
WHERE industry ='';

FROM layoffs_staging2                            -- Check for Null values
WHERE industry IS NULL;


SELECT *                                          
FROM layoffs_staging2
WHERE company = 'Airbnb';

SELECT t1.industry, t2.industry                 -- auto populate NULL cells
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company 
WHERE (t1.industry IS NULL)
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging t1							-- Update the table
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL )
AND t2.industry IS NOT NULL;

DELETE 									  			-- Delete null values
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 									  			-- Delete null values
FROM layoffs_staging2
WHERE `Date` IS NULL;

ALTER TABLE Layoffs_staging2                         -- Drop row_num
DROP COLUMN row_num;


-- Exploratory Analysis
SELECT *                                          
FROM layoffs_staging2;

SELECT MAX(total_laid_off)
FROM layoffs_staging2;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT `date` , SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY `date`
ORDER BY 2 DESC;

SELECT YEAR(`date`) , SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 2 DESC;

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

SELECT MONTH(`date`) , SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY MONTH(`date`)
ORDER BY 1 DESC;

SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY `MONTH`
ORDER BY 1 DESC;

SELECT `MONTH`, SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;

WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
GROUP BY `MONTH`
ORDER BY 1 DESC
)
SELECT `MONTH`, total_off
,SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;


SELECT country, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country, YEAR(`date`)
ORDER BY 3 DESC;

WITH Company_Year (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS
(SELECT *,
 DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT Null
)
SELECT * 
FROM Company_Year_Rank
WHERE Ranking <= 5;