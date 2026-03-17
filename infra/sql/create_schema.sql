-- =============================================================================
-- create_schema.sql
-- Run once per environment. Creates the DEMO_BSC schema and a read-only role.
-- =============================================================================

USE DATABASE global_supply_chain;

CREATE SCHEMA IF NOT EXISTS DEMO_BSC
    COMMENT = 'BSC Order Status Assistant demo schema';

-- Demo role with read-only access on DEMO_BSC
CREATE ROLE IF NOT EXISTS DEMO_ROLE;

GRANT USAGE ON DATABASE global_supply_chain TO ROLE DEMO_ROLE;
GRANT USAGE ON SCHEMA DEMO_BSC TO ROLE DEMO_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA DEMO_BSC TO ROLE DEMO_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA DEMO_BSC TO ROLE DEMO_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA DEMO_BSC TO ROLE DEMO_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA DEMO_BSC TO ROLE DEMO_ROLE;

-- Allow DEMO_ROLE to call Cortex
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE DEMO_ROLE;

