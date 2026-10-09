//Snowflake Data Engineering using SQL

// Prepare Database & Table
CREATE OR REPLACE DATABASE COPY_DB
CREATE OR REPLACE TABLE COPY_DB.PUBLIC.ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT VARCHAR(30),
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30)
);

// Prepare stage Object from AWS bucket
CREATE OR REPLACE STAGE COPY_DB.PUBLIC.aws_stage_copy
    url='s3://snowflakebucket-copyoption/size/';
LIST @COPY_DB.PUBLIC.aws_stage_copy

// Load Data into table
COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    file_format = (type=csv field_delimiter=',' skip_header=1)
    pattern = '.*Orders.*'
    VALIDATION_MODE = RETURN_ERRORS;

Select * From ORDERS
-------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------

//Load Unstructured Data like Jason File

// 1st Step: Create new path staging to AWS bucket
CREATE OR REPLACE stage MANAGE_DB.EXTERNAL_STAGE.JSONSTAGE
     url='s3://bucketsnowflake-jsondemo';

//Create SCHEMA
SHOW SCHEMAS IN DATABASE MANAGE_DB;
CREATE SCHEMA FILE_FORMATS
CREATE OR REPLACE file format MANAGE_DB.FILE_FORMATS.JSONFORMAT
    TYPE = JSON;
    
// CREATE Table for json file consist only 1 row
CREATE OR REPLACE table OUR_FIRST_DB.PUBLIC.JSON_RAW (
    raw_file variant);
select * from OUR_FIRST_DB.PUBLIC.JSON_RAW


// Loading data from S3 bucket to snoflake storage Data base    
COPY INTO OUR_FIRST_DB.PUBLIC.JSON_RAW
    FROM @MANAGE_DB.EXTERNAL_STAGE.JSONSTAGE
    file_format= MANAGE_DB.FILE_FORMATS.JSONFORMAT
    files = ('HR_data.json');
    
 // See data after loading  
SELECT * FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

// 2nd Step : Parsing JSON & analyse Raw JASON and insert it into HR_DATA, ETL Process
CREATE OR REPLACE TABLE HR_DATA AS
SELECT
    RAW_FILE:id::int as id,
    RAW_FILE:first_name::STRING as first_name,
    RAW_FILE:last_name::STRING as last_name,
    RAW_FILE:gender::STRING as gender,
    RAW_FILE:city::STRING as city,
    
    //Handling Nested data in JASON
    RAW_FILE:job.salary::INT as salary,
    RAW_FILE:job.title::STRING as title,

    //Handling Array in JASON
    f.value:language::STRING as First_language,
    f.value:level::STRING as Level_spoken
from OUR_FIRST_DB.PUBLIC.JSON_RAW, table(flatten(RAW_FILE:spoken_languages)) f;

// Final step: See the Data
Select * FROM OUR_FIRST_DB.PUBLIC.HR_DATA;

--------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------
//CREATING Pipline using Snowpipe. Need onfigure also for S3 event notification in AWS cloud Using ARN Code from Snowflake 

// Step1: Create table first
CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.employees (
  id INT,
  first_name STRING,
  last_name STRING,
  email STRING,
  location STRING,
  department STRING
  );
    

// Create file format object
CREATE OR REPLACE file format MANAGE_DB.file_formats.csv_fileformat
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE;
    
    
 // Create stage object with integration object & file format object
CREATE OR REPLACE stage MANAGE_DB.external_stages.csv_folder
    URL = 's3://snowflakes3bucket123/csv/snowpipe'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = MANAGE_DB.file_formats.csv_fileformat;
   

 // Create stage object with integration object & file format object
LIST @MANAGE_DB.external_stages.csv_folder;


// Create schema to keep things organized
CREATE OR REPLACE SCHEMA MANAGE_DB.pipes;

// Define pipe
CREATE OR REPLACE pipe MANAGE_DB.pipes.employee_pipe
auto_ingest = TRUE
AS
COPY INTO OUR_FIRST_DB.PUBLIC.employees
FROM @MANAGE_DB.external_stages.csv_folder ;

// Describe pipe
DESC pipe employee_pipe;
    
SELECT * FROM OUR_FIRST_DB.PUBLIC.employees ;

/*------------------------------------------------------------------------------------------------------------------------------------------*/

select 
title,
CASE 
	WHEN rating IN ('PG','PG-13')  OR length > 210 THEN 'Great Rating or Long (Tier1)' 
	WHEN description like '%Drama%' AND length > 90 THEN 'Long Drama tier2'
	WHEN description like '%Drama%' THEN 'Short Drama tier3'
	WHEN rental_rate<1 THEN 'Very cheap (tier 4)'
END as Tier_List
from film
WHERE
CASE 
	WHEN rating IN ('PG','PG-13')  OR length > 210 THEN 'Great Rating or Long (Tier1)' 
	WHEN description like '%Drama%' AND length > 90 THEN 'Long Drama tier2'
	WHEN description like '%Drama%' THEN 'Short Drama tier3'
	WHEN rental_rate<1 THEN 'Very cheap (tier 4)'
END is not NUL

SELECT
title,
CASE
WHEN rating IN ('PG','PG-13') OR length > 210 THEN 'Great rating or long (tier 1)'
WHEN description LIKE '%Drama%' AND length>90 THEN 'Long drama (tier 2)'
WHEN description LIKE '%Drama%' THEN 'Short drama (tier 3)'
WHEN rental_rate<1 THEN 'Very cheap (tier 4)'
END as tier_list
FROM film
WHERE 
CASE
WHEN rating IN ('PG','PG-13') OR length > 210 THEN 'Great rating or long (tier 1)'
WHEN description LIKE '%Drama%' AND length>90 THEN 'Long drama (tier 2)'
WHEN description LIKE '%Drama%' THEN 'Short drama (tier 3)'
WHEN rental_rate<1 THEN 'Very cheap (tier 4)'
END is not null


--------------------------------------------------------------------------------------

select *
from public.payment

select
customer_id,
Date(payment_date),
Round(AVG(anount),2) as avg_amount,
count(*)
from public.payment
whe
where date 

-------------------------------------------------------------------------------------

select * from customer
select * from payment
select * from address
---------------------------------------------------------------------------------------
/* Using subquery */
    
select first_name, last_name from customer
where customer_id in
(select customer_id from payment
where date(payment_date) = '2020-01-25')

----------------------------------------------------------------------------------

select first_name, last_name, email from customer
where customer_id in
	(select customer_id from payment
	group by customer_id
	having sum(amount) >100
	)
AND customer_id in
	(SELECT customer_id from customer A
	INNER JOIN address B
	ON A.address_id = B.address_id
	WHERE B.district = 'California'
	)
---------------------------------------------------------------------------------------------
select ROUND(avg(daily_revenue),2)
from
(select DATE(payment_date), sum(amount) as daily_revenue
from payment
Group by Date(payment_date)) As Sub


---------------------------------------------------------------------------------------------

select 
*, (select Max(amount) from payment)-amount As difference
from payment

--------------------------------------------------------------------------------------------

select * from film

---------------------------------------------------------------------------------------------

select title, film_id, replacement_cost,rating
from film A
WHERE replacement_cost = (select min(replacement_cost) from film B Where A.rating = B.rating)

-----------------------------------------------------------------------------------------------
select C.fare_conditions, count(*) from boarding_passes A
INNER JOIN flights B
ON A.flight_id = B.flight_id
INNER JOIN seats C
ON B.aircraft_code = C.aircraft_code AND A.seat_no = C.seat_no
GROUP BY C.fare_conditions
ORDER BY 2 DESC
