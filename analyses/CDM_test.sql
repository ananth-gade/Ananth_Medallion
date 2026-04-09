-- ============================================
-- Set context
-- ============================================
USE DATABASE WORKDAY;
USE SCHEMA ODS;

-- ============================================
-- Create CUSTOMER table
-- ============================================
CREATE OR REPLACE TABLE WORKDAY.ODS.CUSTOMER (
    CUSTOMER_ID       INT            NOT NULL,
    FIRST_NAME        VARCHAR(100)   NOT NULL,
    LAST_NAME         VARCHAR(100)   NOT NULL,
    EMAIL             VARCHAR(255),
    PHONE             VARCHAR(20),
    DATE_OF_BIRTH     DATE,
    CREATED_AT        TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT        TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (CUSTOMER_ID)
);

-- ============================================
-- Create CUSTOMER_ADDRESS table
-- ============================================
CREATE OR REPLACE TABLE WORKDAY.ODS.CUSTOMER_ADDRESS (
    ADDRESS_ID        INT            NOT NULL,
    CUSTOMER_ID       INT            NOT NULL,
    ADDRESS_TYPE      VARCHAR(20)    DEFAULT 'HOME',   -- HOME, WORK, BILLING, SHIPPING
    ADDRESS_LINE_1    VARCHAR(255)   NOT NULL,
    ADDRESS_LINE_2    VARCHAR(255),
    CITY              VARCHAR(100)   NOT NULL,
    STATE             VARCHAR(50),
    ZIP_CODE          VARCHAR(20),
    COUNTRY           VARCHAR(100)   DEFAULT 'US',
    IS_PRIMARY        BOOLEAN        DEFAULT FALSE,
    CREATED_AT        TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT        TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (ADDRESS_ID),
    FOREIGN KEY (CUSTOMER_ID) REFERENCES WORKDAY.ODS.CUSTOMER(CUSTOMER_ID)
);

-- ============================================
-- Insert sample data into CUSTOMER
-- ============================================
INSERT INTO WORKDAY.ODS.CUSTOMER (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE, DATE_OF_BIRTH)
VALUES
    (1, 'John',    'Smith',    'john.smith@email.com',    '555-0101', '1985-03-15'),
    (2, 'Sarah',   'Johnson',  'sarah.j@email.com',       '555-0102', '1990-07-22'),
    (3, 'Michael', 'Williams', 'michael.w@email.com',     '555-0103', '1978-11-08'),
    (4, 'Emily',   'Brown',    'emily.brown@email.com',   '555-0104', '1995-01-30'),
    (5, 'David',   'Jones',    'david.jones@email.com',   '555-0105', '1982-09-12'),
    (6, 'Lisa',    'Davis',    'lisa.davis@email.com',     '555-0106', '1988-06-25'),
    (7, 'Robert',  'Miller',   'robert.m@email.com',      '555-0107', '1975-12-03'),
    (8, 'Amanda',  'Wilson',   'amanda.w@email.com',      '555-0108', '1992-04-18'),
    (9, 'James',   'Taylor',   'james.t@email.com',       '555-0109', '1980-08-07'),
   (10, 'Jessica', 'Anderson', 'jessica.a@email.com',     '555-0110', '1997-02-14');

-- ============================================
-- Insert sample data into CUSTOMER_ADDRESS
-- ============================================
INSERT INTO WORKDAY.ODS.CUSTOMER_ADDRESS (ADDRESS_ID, CUSTOMER_ID, ADDRESS_TYPE, ADDRESS_LINE_1, ADDRESS_LINE_2, CITY, STATE, ZIP_CODE, COUNTRY, IS_PRIMARY)
VALUES
    (1,  1, 'HOME',     '123 Main St',        'Apt 4B',   'New York',      'NY', '10001', 'US', TRUE),
    (2,  1, 'WORK',     '456 Corporate Blvd',  NULL,       'New York',      'NY', '10018', 'US', FALSE),
    (3,  2, 'HOME',     '789 Oak Avenue',      NULL,       'Los Angeles',   'CA', '90001', 'US', TRUE),
    (4,  2, 'SHIPPING', '101 Warehouse Dr',    'Suite 5',  'Los Angeles',   'CA', '90015', 'US', FALSE),
    (5,  3, 'HOME',     '222 Pine Road',       NULL,       'Chicago',       'IL', '60601', 'US', TRUE),
    (6,  4, 'HOME',     '333 Elm Street',      'Unit 12',  'Houston',       'TX', '77001', 'US', TRUE),
    (7,  4, 'BILLING',  '444 Commerce Way',    NULL,       'Houston',       'TX', '77002', 'US', FALSE),
    (8,  5, 'HOME',     '555 Cedar Lane',      NULL,       'Phoenix',       'AZ', '85001', 'US', TRUE),
    (9,  6, 'HOME',     '666 Birch Drive',     'Apt 3A',   'Philadelphia',  'PA', '19101', 'US', TRUE),
   (10,  6, 'WORK',     '777 Market Street',   'Floor 8',  'Philadelphia',  'PA', '19103', 'US', FALSE),
   (11,  7, 'HOME',     '888 Maple Court',     NULL,       'San Antonio',   'TX', '78201', 'US', TRUE),
   (12,  8, 'HOME',     '999 Walnut Blvd',     NULL,       'San Diego',     'CA', '92101', 'US', TRUE),
   (13,  8, 'SHIPPING', '100 Delivery Ave',    'Dock 2',   'San Diego',     'CA', '92102', 'US', FALSE),
   (14,  9, 'HOME',     '111 Spruce Way',      NULL,       'Dallas',        'TX', '75201', 'US', TRUE),
   (15, 10, 'HOME',     '121 Willow Path',     'Suite 1',  'San Jose',      'CA', '95101', 'US', TRUE);

-- ============================================
-- Verify the data
-- ============================================
SELECT 'CUSTOMER' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM WORKDAY.ODS.CUSTOMER
UNION ALL
SELECT 'CUSTOMER_ADDRESS', COUNT(*) FROM WORKDAY.ODS.CUSTOMER_ADDRESS;

SELECT * FROM WORKDAY.ODS.CUSTOMER;

SELECT * FROM WORKDAY.ODS.CUSTOMER_ADDRESS;

select 
FIRST_NAME, LAST_NAME, EMAIL, PHONE, DATE_OF_BIRTH, ADDRESS_LINE_1, CITY, STATE, ZIP_CODE
from CDM.workforce.CDM_CUSTOMER where customer_id = 1;

-- Simulate an address-only change
UPDATE WORKDAY.ODS.CUSTOMER_ADDRESS 
SET CITY = 'Brooklyn', UPDATED_AT = CURRENT_TIMESTAMP() 
WHERE ADDRESS_ID = 1;

SELECT CUSTOMER_ID, FIRST_NAME, LAST_NAME, ADDRESS_ID, CITY, CDM_UPDATE_TIMESTAMP
FROM CDM.WORKFORCE.CDM_CUSTOMER 
WHERE CUSTOMER_ID = 1;

-- ============================================
-- Simulate updates to test incremental logic
-- ============================================

-- Update 1: Customer email change (customer-only change)
UPDATE WORKDAY.ODS.CUSTOMER
SET PHONE = '111-9999', UPDATED_AT = CURRENT_TIMESTAMP()
WHERE CUSTOMER_ID = 2;

-- Update 2: Address change (address-only change)
UPDATE WORKDAY.ODS.CUSTOMER_ADDRESS
SET ADDRESS_LINE_1 = '999 Sunset Blvd', CITY = 'Beverly Hills', ZIP_CODE = '90210', UPDATED_AT = CURRENT_TIMESTAMP()
WHERE ADDRESS_ID = 3;

-- Update 3: Both customer and address change for same person
UPDATE WORKDAY.ODS.CUSTOMER
SET PHONE = '111-9999', UPDATED_AT = CURRENT_TIMESTAMP()
WHERE CUSTOMER_ID = 5;

UPDATE WORKDAY.ODS.CUSTOMER_ADDRESS
SET STATE = 'IL', CITY = 'Naperville', UPDATED_AT = CURRENT_TIMESTAMP()
WHERE ADDRESS_ID = 8;

-- Verify source changes
SELECT 'CUSTOMER' AS SOURCE, CUSTOMER_ID, EMAIL, PHONE, UPDATED_AT
FROM WORKDAY.ODS.CUSTOMER WHERE CUSTOMER_ID IN (2, 5);

SELECT 'ADDRESS' AS SOURCE, ADDRESS_ID, CUSTOMER_ID, ADDRESS_LINE_1, CITY, STATE, ZIP_CODE, UPDATED_AT
FROM WORKDAY.ODS.CUSTOMER_ADDRESS WHERE ADDRESS_ID IN (3, 8);