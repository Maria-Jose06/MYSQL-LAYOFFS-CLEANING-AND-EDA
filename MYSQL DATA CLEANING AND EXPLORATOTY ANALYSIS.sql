SELECT *
FROM layoffs;


-- DATA CLEANING

SELECT *, 
ROW_NUMBER () OVER (
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`
) AS row_numb2
FROM layoffs_staging;


-- STAGING TABLE 2

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
  row_numb INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT *, 
ROW_NUMBER () OVER (
PARTITION BY company, industry, location, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
) AS row_numb
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2;


-- INDUSTRY STANDIRIZING

SELECT industry
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';


-- COMPANY TRIM

SELECT company, TRIM(company) 
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT company 
FROM layoffs_staging2;


-- COUNTRY TRIM

SELECT country 
FROM layoffs_staging2
WHERE country LIKE '%.';

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT country 
FROM layoffs_staging2
ORDER BY 1;


-- CHECKING OTHER COLUMNS

SELECT location 
FROM layoffs_staging2
ORDER BY 1;

SELECT country 
FROM layoffs_staging2
ORDER BY 1;


-- DATE

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y') 
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;
	

-- DELETE DUPLICATES

SELECT *
FROM layoffs_staging2
WHERE row_numb > 1;

DELETE 
FROM layoffs_staging2
WHERE row_numb > 1;


-- CHANGE INDUSTRY NULLS

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
AND industry = '';

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
  AND t2.industry IS NOT NULL;
  
UPDATE layoffs_staging2 t1
 JOIN layoffs_staging2 t2
    ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
  AND t2.industry IS NOT NULL;
  
SELECT t1.industry, t2.industry
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
    ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;
  
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';  
  
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2;


-- DELETE UNNECESSARY COLUMNS

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_numb;


-- EXPLORATORY DATA ANALYSIS

SELECT * 
FROM layoffs_staging2;


-- PERCENTAGE LAID OFF

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

SELECT company, SUM(total_laid_off) AS sum
FROM layoffs_staging2
GROUP BY company 
ORDER BY 2 DESC;

SELECT industry, SUM(total_laid_off) AS sum
FROM layoffs_staging2
GROUP BY industry 
ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off) AS sum
FROM layoffs_staging2
GROUP BY country 
ORDER BY 2 DESC;

SELECT YEAR(`date`), SUM(total_laid_off) AS sum
FROM layoffs_staging2
GROUP BY YEAR (`date`)
ORDER BY 1 DESC;

SELECT stage, SUM(total_laid_off) AS sum
FROM layoffs_staging2
GROUP BY stage
ORDER by 1 DESC;

SELECT company, SUM(percentage_laid_off) AS sum
FROM layoffs_staging2
GROUP BY company 
ORDER BY 2 DESC;


SELECT company, AVG(percentage_laid_off) AS avg
FROM layoffs_staging2
GROUP BY company 
ORDER BY 2 DESC;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;


-- ROLLING TOTAL LAID OFFSS

SELECT SUBSTRING(`date`, 1, 7) AS `Month`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 ASC;

WITH rolling_total AS 
(
SELECT SUBSTRING(`date`, 1, 7) AS `Month`, SUM(total_laid_off) AS total_offs
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 ASC
)
SELECT `Month`, total_offs, SUM(total_offs) OVER(ORDER BY `Month`) AS rolling_total_offs
FROM rolling_total;

WITH company_year AS 
(
    SELECT company, YEAR(`date`) AS `YEAR`, SUM(total_laid_off) AS laid_offs_per
    FROM layoffs_staging2
    GROUP BY company, YEAR(`date`)
)
SELECT `YEAR`, laid_offs_per, SUM(laid_offs_per) OVER(ORDER BY `YEAR`) AS rolling_per_offs
FROM company_year;

WITH company_year (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
), 
company_year_Rank AS
(
SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS ranking 
FROM company_year
WHERE years IS NOT NULL
)
SELECT * 
FROM company_year_Rank
WHERE ranking <= 5;







