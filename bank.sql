create database test
create schema test_schema


create database bank 

use database bank

CREATE TABLE DISTRICT(
District_Code INT PRIMARY KEY	,
District_Name VARCHAR(100)	,
Region VARCHAR(100)	,
No_of_inhabitants	INT,
No_of_municipalities_with_inhabitants_less_499 INT,
No_of_municipalities_with_inhabitants_500_btw_1999	INT,
No_of_municipalities_with_inhabitants_2000_btw_9999	INT,
No_of_municipalities_with_inhabitants_less_10000 INT,	
No_of_cities	INT,
Ratio_of_urban_inhabitants	FLOAT,
Average_salary	INT,
No_of_entrepreneurs_per_1000_inhabitants	INT,
No_committed_crime_2017	INT,
No_committed_crime_2018 INT
) ;

CREATE OR REPLACE TABLE ACCOUNT(
account_id INT PRIMARY KEY,
district_id	INT,
frequency	VARCHAR(100),
`Date` DATE ,
Account_type VARCHAR(100),
FOREIGN KEY (district_id) references DISTRICT(District_Code) 
);

CREATE TABLE ORDER_LIST(
order_id	INT PRIMARY KEY,
account_id	INT,
bank_to	VARCHAR(45),
account_to	INT,
amount FLOAT,
FOREIGN KEY (account_id) references ACCOUNT(account_id)
);



CREATE OR REPLACE TABLE LOAN(
loan_id	INT ,
account_id	INT,
`Date`	DATE,
amount	INT,
duration	INT,
payments	INT,
`status` VARCHAR(500),
FOREIGN KEY (account_id) references ACCOUNT(account_id)
);

CREATE TABLE TRANSACTIONS(
trans_id INT,	
account_id	INT,
`Date`	DATE,
`Type`	VARCHAR(30),
operation	VARCHAR(40),
amount	INT,
balance	FLOAT,
Purpose	VARCHAR(40),
bank	VARCHAR(45),
`account` INT,
FOREIGN KEY (account_id) references ACCOUNT(account_id));

CREATE OR REPLACE TABLE CLIENT(
client_id	INT PRIMARY KEY,

Birth_date	DATE,
Sex	CHAR(50),
district_id INT,
FOREIGN KEY (district_id) references DISTRICT(District_Code) 
);

CREATE TABLE DISPOSITION(
disp_id	INT PRIMARY KEY,
client_id INT,
account_id	INT,
`type` CHAR(15),
FOREIGN KEY (account_id) references ACCOUNT(account_id),
FOREIGN KEY (client_id) references CLIENT(client_id)
);

CREATE OR REPLACE TABLE CARD(
card_id	INT PRIMARY KEY,
disp_id	INT,
`type` CHAR(50)	,
issued DATE,
FOREIGN KEY (disp_id) references DISPOSITION(disp_id)
);



create or replace storage integration s3_int
type = external_stage
storage_provider =s3
enabled =true
storage_aws_role_arn='arn:aws:iam::483550710521:role/yash'
storage_allowed_locations=('s3://checkbank/');



describe storage integration s3_int


CREATE OR REPLACE FILE FORMAT CSV
   TYPE = 'CSV'
   FIELD_DELIMITER = ','
   compression = auto
   SKIP_HEADER = 1;


CREATE OR REPLACE STAGE BANK
URL ='s3://checkbank/'
--credentials=(aws_key_id='AKIAXQKR3H3PSG72XFMK'aws_secret_key='eKL6a6FjlQHic4s8Ne712Aelzg2ou4j6tNsVvFq5')
file_format= CSV
storage_integration = s3_int;

list @BANK

show stages  BANK



CREATE OR REPLACE PIPE BANK_SNOWPIPE_DISTRICT AUTO_INGEST = TRUE AS
COPY INTO "BANK"."PUBLIC"."DISTRICT" --yourdatabase -- your schema ---your table
FROM '@BANK/district/' --s3 bucket subfolde4r name
FILE_FORMAT = CSV;

CREATE OR REPLACE PIPE BANK_SNOWPIPE_ACCOUNT AUTO_INGEST = TRUE AS
COPY INTO "BANK"."PUBLIC"."ACCOUNT"
FROM '@BANK/accounts/'
FILE_FORMAT = CSV;

CREATE OR REPLACE PIPE BANK_SNOWPIPE_TXNS AUTO_INGEST = TRUE AS
COPY INTO "BANK"."PUBLIC"."TRANSACTIONS"
FROM '@BANK/txn/'
FILE_FORMAT = CSV;

CREATE OR REPLACE PIPE BANK_SNOWPIPE_DISP AUTO_INGEST = TRUE AS
COPY INTO "BANK"."PUBLIC"."DISPOSITION"
FROM '@BANK/disp/'
FILE_FORMAT = CSV;

CREATE OR REPLACE PIPE BANK_SNOWPIPE_CARD AUTO_INGEST = TRUE AS
COPY INTO "BANK"."PUBLIC"."CARD"
FROM '@BANK/card/'
FILE_FORMAT = CSV;

CREATE OR REPLACE PIPE BANK_SNOWPIPE_ORDER_LIST AUTO_INGEST = TRUE AS
COPY INTO "BANK"."PUBLIC".ORDER_LIST
FROM '@BANK/order/'
FILE_FORMAT = CSV;

CREATE OR REPLACE PIPE BANK_SNOWPIPE_LOAN AUTO_INGEST = TRUE AS
COPY INTO "BANK"."PUBLIC"."LOAN"
FROM '@BANK/loan/'
FILE_FORMAT = CSV;

CREATE OR REPLACE PIPE BANK_SNOWPIPE_CLIENT AUTO_INGEST = TRUE AS
COPY INTO "BANK"."PUBLIC"."CLIENT"
FROM '@BANK/client/'
FILE_FORMAT = CSV;

SHOW PIPES;

SELECT count(*) FROM DISTRICT;
SELECT count(*) FROM ACCOUNT;
SELECT count(*) FROM TRANSACTIONS;
SELECT count(*) FROM DISPOSITION;
SELECT count(*) FROM CARD;
SELECT count(*) FROM ORDER_LIST;
SELECT count(*) FROM LOAN;
SELECT count(*) FROM CLIENT;

ALTER PIPE BANK_SNOWPIPE_DISTRICT refresh;

ALTER PIPE BANK_SNOWPIPE_ACCOUNT refresh;

ALTER PIPE BANK_SNOWPIPE_TXNS refresh;

ALTER PIPE BANK_SNOWPIPE_DISP refresh;

ALTER PIPE BANK_SNOWPIPE_CARD refresh;

ALTER PIPE BANK_SNOWPIPE_ORDER_LIST refresh;

ALTER PIPE BANK_SNOWPIPE_LOAN refresh;

ALTER PIPE BANK_SNOWPIPE_CLIENT refresh;
-----------------------------------------------------------------------------------------------------------------------------------------------
SELECT * FROM DISTRICT;
SELECT * FROM ACCOUNT;
SELECT * FROM TRANSACTIONS;
SELECT * FROM DISPOSITION;
SELECT * FROM CARD;
SELECT * FROM ORDER_LIST;
SELECT * FROM LOAN;
SELECT * FROM CLIENT;

