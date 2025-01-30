-- SQL Data Cleaning Project: "World Layoffs"

-- Step 1: Create a Staging Table for Data Cleaning (Backup of the Original Data)
CREATE TABLE layoff_staging LIKE layoffs;

-- Insert all data from the original table into the staging table
INSERT INTO layoff_staging SELECT * FROM layoffs;

-- View the staging data
SELECT * FROM layoff_staging;


-- Step 2: Identify Duplicate Records using ROW_NUMBER()
WITH duplicate_cte AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
        ) AS row_num
    FROM layoff_staging
)
SELECT * FROM duplicate_cte WHERE row_num > 1; -- Check duplicate records


-- Step 3: Remove Duplicates (Since MySQL does not support DELETE in CTEs)

-- Create a new cleaned table
CREATE TABLE `layoff_staging2` (
    `company` TEXT,
    `location` TEXT,
    `industry` TEXT,
    `total_laid_off` INT DEFAULT NULL,
    `percentage_laid_off` TEXT,
    `date` TEXT,
    `stage` TEXT,
    `country` TEXT,
    `funds_raised_millions` INT DEFAULT NULL,
    `row_num` INT
);

-- Insert data with row numbers to identify duplicates
INSERT INTO layoff_staging2
SELECT *,
    ROW_NUMBER() OVER (
        PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
    ) AS row_num
FROM layoff_staging;

-- Delete duplicate records (keeping only row_num = 1)
DELETE FROM layoff_staging2 WHERE row_num > 1;


-- Step 4: Standardizing Data

-- Trim extra spaces from company names
UPDATE layoff_staging2
SET company = TRIM(company);

-- Standardize Industry Names (Example: All Crypto-related industries become "Crypto")
UPDATE layoff_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Standardize country names (Remove trailing dots, extra spaces)
UPDATE layoff_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Convert 'date' column to proper DATE format
UPDATE layoff_staging2
SET `date` = STR_TO_DATE(date, '%m/%d/%Y');

-- Modify the column to enforce DATE data type
ALTER TABLE layoff_staging2 
MODIFY COLUMN `date` DATE;


-- Step 5: Handling NULL / Missing Values

-- Fill missing industry values using the same company’s existing industry
UPDATE layoff_staging2 t1
JOIN layoff_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;

-- Delete records where both 'total_laid_off' and 'percentage_laid_off' are NULL
DELETE FROM layoff_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;


-- Step 6: Final Cleanup

-- Drop the helper column (row_num) as it's no longer needed
ALTER TABLE layoff_staging2 DROP COLUMN row_num;

-- View the final cleaned dataset
SELECT * FROM layoff_staging2;
