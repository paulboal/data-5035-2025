-------------------------------------------------------------------------------
-- U71 DATA 5035 Snowflake Setup Script
--
-- This script creates several security and database objects in which you
-- will do the exercises for this class. We create separate objects so the
-- work for this class is isolated and can be easily dropped and recreated
-- if necessary.
-- 
-- Security Role: data5035_role
-- User:          data5035_user
-- Warehouse:     data5035_wh
-- Database:      data5035
--
-- You can run this script either by copying / pasting into a Snowflake
-- web session (Snowsight) or by using the Snowflake extension in VSCode.
-------------------------------------------------------------------------------

-- data5035 user and role
USE ROLE securityadmin;
CREATE OR REPLACE ROLE data5035_role;
CREATE OR REPLACE USER data5035_user PASSWORD = "***";
GRANT ROLE data5035_role TO USER data5035_user;
GRANT ROLE data5035_role TO ROLE sysadmin;

-- data5035 warehouse and database
USE ROLE sysadmin;
CREATE OR REPLACE WAREHOUSE data5035_wh  WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 1 INITIALLY_SUSPENDED = TRUE;
GRANT ALL ON WAREHOUSE data5035_wh  TO ROLE data5035_role;
CREATE OR REPLACE DATABASE data5035; 
GRANT ALL ON DATABASE data5035 TO ROLE data5035_role;
GRANT ALL ON ALL SCHEMAS IN DATABASE data5035 TO ROLE data5035_role;

