-- MARKET ANALYSIS DATA EXPLORATION

-- 1. Dataset Structure Inspection
-- Retrieve column names and data types to understand dataset schema
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'market_analysis';


-- 2. Preliminary Data Sampling
-- Display initial rows to preview dataset content
SELECT *
FROM market_analysis
LIMIT 5;


-- 3. Data Quality Assessment
-- Identify missing values across columns to evaluate data completeness
SELECT 
    COUNT(CASE WHEN lead_time IS NULL THEN 1 END) AS missing_lead_time,
    COUNT(CASE WHEN nightly_rate IS NULL THEN 1 END) AS missing_nightly_rate,
    COUNT(CASE WHEN length_stay IS NULL THEN 1 END) AS missing_length_stay
FROM market_analysis;


-- 4. Null Value Resolution
-- Substitute null entries with default value for data consistency
-- Option 1: Using COALESCE
SELECT COALESCE(lead_time, '0') AS lead_time_filled
FROM market_analysis;

-- Option 2: Using CASE
SELECT 
    CASE 
        WHEN lead_time IS NULL THEN '0' 
        ELSE lead_time 
    END AS lead_time_filled
FROM market_analysis;


-- 5. Duplicate Record Management
-- Detect and eliminate duplicate records to ensure data integrity
WITH duplicate_records AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY unified_id ORDER BY (SELECT NULL)) AS row_number
    FROM market_analysis
)
DELETE FROM market_analysis
WHERE unified_id IN (
    SELECT unified_id
    FROM duplicate_records
    GROUP BY unified_id
    HAVING COUNT(row_number) > 1
);


--------------------------------------------------------------------
-- 6. **Revenue Metrics**
--------------------------------------------------------------------

-- 6-1. Total Markets Data:
-- Create temporary table to union all tables market analysis
-- (using CTE and Temp Table)
DROP TABLE IF EXISTS total_market_analysis;
CREATE TEMPORARY TABLE total_market_analysis (
    unified_id VARCHAR(50),
    date_month VARCHAR(50),
    city VARCHAR(50),
    host_type VARCHAR(50),
    revenue INT,
    openness INT,
    occupancy FLOAT,
    nightly_rate FLOAT,
    lead_time FLOAT,
    length_stay FLOAT
); 

WITH all_market AS (
    SELECT * FROM market_analysis
    UNION
    SELECT * FROM market_analysis_2019
)

INSERT INTO total_market_analysis (unified_id, date_month, city, host_type, revenue, openness, occupancy, nightly_rate, lead_time, length_stay)
SELECT unified_id, "month", city, host_type, revenue, openness , occupancy , nightly_rate , lead_time , length_stay 
FROM all_market;


-- 6-2. Total Revenue:
SELECT SUM(revenue) AS total_revenue
FROM total_market_analysis;


-- 6-3. Average Monthly Revenue per Listing:
SELECT TO_DATE(date_month, 'YYYY-MM') AS sales_date, SUM(revenue) / COUNT(unified_id) AS avg_revenue
FROM total_market_analysis
GROUP BY date_month
ORDER BY sales_date;


-- 6-4. Revenue Growth Rate per Month:
WITH growth_rate AS (
    SELECT 
        TO_DATE(date_month, 'YYYY-MM') AS sales_month,
        SUM(CAST(revenue AS FLOAT)) AS current_revenue,
        LAG(SUM(CAST(revenue AS FLOAT))) OVER (ORDER BY TO_DATE(date_month, 'YYYY-MM')) AS pre_revenue
    FROM 
        total_market_analysis
    GROUP BY 
        sales_month
    ORDER BY 
        sales_month
)

SELECT 
    sales_month,
    CASE 
        WHEN pre_revenue IS NULL THEN 100
        WHEN pre_revenue = 0 THEN 100
        ELSE ROUND(((current_revenue - pre_revenue) / pre_revenue)::numeric, 3) * 100
    END AS growth
FROM 
    growth_rate;


-- 6-5. City Revenue per Month:
SELECT city, TO_DATE(date_month, 'YYYY-MM') AS sales_date, SUM(revenue) AS revenue 
FROM market_analysis ma
GROUP BY city , TO_DATE(date_month, 'YYYY-MM')
ORDER BY TO_DATE(date_month, 'YYYY-MM'), city

--------------------------------------------------------------------
-- 7. **Occupancy Metrics**
--------------------------------------------------------------------

-- 7-1. Occupancy Rate:
-- Percentage of occupied nights out of total available nights in a month.
SELECT TO_DATE(date_month, 'YYYY-MM') AS sales_date, 
CAST((SUM(openness * occupancy) / SUM(openness)) * 100 AS NUMERIC(10,2)) AS Occupancy_Rate
FROM total_market_analysis 
GROUP BY sales_date
ORDER BY sales_date

    
-- 7-2. Average Length of Stay: 
-- Mean duration of guest stays in nights.
SELECT TO_DATE(date_month, 'YYYY-MM') AS sales_date , 
SUM(length_stay) / COUNT(*) AS Average_Length_Stay
FROM total_market_analysis
GROUP BY sales_date

    
-- 7-3. Occupancy Growth Rate:
WITH occ_growth_rate AS (
SELECT TO_DATE(date_month, 'YYYY-MM') AS sales_date, 
CAST((SUM(openness * occupancy) / SUM(openness)) * 100 AS NUMERIC(10,2)) AS Occupancy_Rate,
LAG(CAST((SUM(openness * occupancy) / SUM(openness)) * 100 AS NUMERIC(10,2))) OVER (ORDER BY TO_DATE(date_month, 'YYYY-MM')) AS previous_rate
FROM total_market_analysis 
GROUP BY sales_date
ORDER BY sales_date
)
SELECT sales_date, 
    CASE
        WHEN previous_rate IS NULL THEN NULL
        WHEN previous_rate = 0 THEN 100
        ELSE CAST(((Occupancy_Rate - previous_rate) / previous_rate) * 100 AS NUMERIC(10,2)) 
    END AS growth_rate
FROM occ_growth_rate

--------------------------------------------------------------------
-- 8. **Pricing Metrics**
--------------------------------------------------------------------

-- 8-1. Average Nightly Rate: 
-- Mean nightly rate charged for listings.
SELECT TO_DATE(date_month, 'YYYY-MM') AS sales_date, SUM(revenue)/SUM(openness * occupancy) AS avg_revenue_per_night
FROM total_market_analysis
GROUP BY sales_date
ORDER BY sales_date

    
-- 8-2. Rate of Price Change: 
-- Percentage change in nightly rates compared to the previous month or year.
WITH price_change AS (
SELECT TO_DATE(date_month, 'YYYY-MM') AS sales_date, SUM(revenue)/SUM(openness * occupancy) AS avg_revenue_per_night,
LAG(SUM(revenue)/SUM(openness * occupancy)) OVER (ORDER BY TO_DATE(date_month, 'YYYY-MM')) AS previous_revenue
FROM total_market_analysis
GROUP BY sales_date
ORDER BY sales_date
)
SELECT sales_date, 
    CASE 
        WHEN previous_revenue IS NULL THEN NULL
        WHEN previous_revenue = 0 THEN 100
        ELSE CAST(((avg_revenue_per_night - previous_revenue)/ previous_revenue) * 100 AS NUMERIC(10,2))
    END AS price_change
FROM price_change

--------------------------------------------------------------------
-- 9. **Demand Metrics**
--------------------------------------------------------------------

-- 9-1. Lead Time: 
-- Average number of days between booking and check-in.
SELECT TO_DATE(date_month, 'YYYY-MM') AS sales_date, AVG(lead_time) AS avg_lead_time
FROM total_market_analysis
GROUP BY sales_date
ORDER BY sales_date

    
-- 9-2. Booking Window: 
-- Distribution of lead times for bookings.

WITH avg_lead_time AS(
SELECT TO_DATE(date_month, 'YYYY-MM') AS sales_date, AVG(lead_time) AS lead_time
FROM total_market_analysis
GROUP BY sales_date
ORDER BY sales_date
)
SELECT sales_date, 
    CASE 
        WHEN lead_time BETWEEN 1 AND 7 THEN '1-7 days'
        WHEN lead_time BETWEEN 8 AND 14 THEN '8-14 days'
        WHEN lead_time BETWEEN 14 AND 21 THEN '2-3 weeks'
        WHEN lead_time BETWEEN 21 AND 28 THEN 'about 1 month'
        ELSE 'more than 1 month'
    END AS booking_window
FROM avg_lead_time
ORDER BY sales_date

--------------------------------------------------------------------
-- 10. **Amenity Impact Metrics**
--------------------------------------------------------------------

-- 10-1. Temp Table For Amenity Data Analysis
DROP TABLE IF EXISTS amen_analysis;
CREATE TEMPORARY TABLE amen_analysis (
    sales_date VARCHAR(50),
    total_sum INT,
    num_amenity INT,
    amenity VARCHAR(50)
);
WITH amen_analysis_data AS (
SELECT TO_DATE(date_month, 'YYYY-MM') AS sales_date, SUM(mark.revenue) AS total_sum, COUNT(hot_tub) AS num_amenity,
CASE 
    WHEN amn.pool = 0 AND amn.hot_tub = 1 THEN 'Hot_tube'
    WHEN amn.pool = 1 AND amn.hot_tub = 0 THEN 'Pool'
    ELSE 'No_Amenity'
END AS amenity
FROM total_market_analysis AS mark
JOIN amenities amn ON mark.unified_id = amn.unified_id 
GROUP BY sales_date, amenity
ORDER BY sales_date, total_sum DESC 
)

INSERT INTO amen_analysis (sales_date, total_sum, num_amenity, amenity)
SELECT sales_date, total_sum, num_amenity, amenity
FROM amen_analysis_data;


-- 10-2. Most Revenue by Amenity:
SELECT sales_date, total_sum, amenity, ROW_NUMBER() OVER (PARTITION BY sales_date ORDER BY total_sum DESC)
FROM amen_analysis
ORDER BY sales_date

    
-- 10-3. Frequency of Listings per Amenity:
-- Proportion of listings offering amenities such as pools or hot tubs.
WITH amn_percent AS(
SELECT sales_date, total_sum, num_amenity, amenity, 
SUM(num_amenity) OVER (PARTITION BY sales_date) AS total_num,
SUM(total_sum) OVER (PARTITION BY sales_date) AS total_revenue
FROM amen_analysis)

SELECT sales_date, total_sum, num_amenity, amenity, total_num, total_revenue,
CAST((CAST(num_amenity AS FLOAT)/CAST(total_num AS FLOAT))* 100 AS NUMERIC(10,2)) AS p_num_amn,
CAST((CAST(total_sum AS FLOAT) / CAST(total_revenue AS FLOAT))* 100 AS NUMERIC(10,2)) AS p_revenue_amn
FROM amn_percent
