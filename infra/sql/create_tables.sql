-- =============================================================================
-- create_tables.sql
-- Core dimension and fact tables for the BSC Order Status Assistant demo.
-- =============================================================================

USE DATABASE global_supply_chain;
USE SCHEMA DEMO_BSC;

-- ---------------------------------------------------------------------------
-- CUSTOMER_DIM
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CUSTOMER_DIM (
    customer_account_id   VARCHAR(20)    NOT NULL PRIMARY KEY,
    customer_name         VARCHAR(200)   NOT NULL,
    customer_name_norm    VARCHAR(200),          -- lower-cased, punctuation stripped
    account_type          VARCHAR(50),           -- HOSPITAL / IDN / CLINIC / GPO / DISTRIBUTOR
    territory             VARCHAR(50),
    sales_region          VARCHAR(50),
    created_at            TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- FACILITY_DIM
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS FACILITY_DIM (
    facility_id           VARCHAR(20)    NOT NULL PRIMARY KEY,
    customer_account_id   VARCHAR(20)    NOT NULL REFERENCES CUSTOMER_DIM(customer_account_id),
    facility_name         VARCHAR(300)   NOT NULL,
    facility_name_norm    VARCHAR(300),          -- normalized for fuzzy search
    facility_name_alt     VARCHAR(300),          -- common abbreviation / alias
    address_line1         VARCHAR(200),
    city                  VARCHAR(100),
    state                 VARCHAR(50),
    zip                   VARCHAR(20),
    country               VARCHAR(50)    DEFAULT 'US',
    created_at            TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- PRODUCT_DIM
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS PRODUCT_DIM (
    product_id            VARCHAR(20)    NOT NULL PRIMARY KEY,
    product_name          VARCHAR(200)   NOT NULL,
    product_family        VARCHAR(100),          -- e.g. CRM / EP / Neuromod / Endoscopy
    product_line          VARCHAR(100),
    unit_cost_usd         DECIMAL(12, 2),
    created_at            TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- CONTACT_DIM  (optional — for contact-name search)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CONTACT_DIM (
    contact_id            VARCHAR(20)    NOT NULL PRIMARY KEY,
    customer_account_id   VARCHAR(20)    REFERENCES CUSTOMER_DIM(customer_account_id),
    facility_id           VARCHAR(20)    REFERENCES FACILITY_DIM(facility_id),
    contact_name          VARCHAR(200),
    contact_name_norm     VARCHAR(200),
    contact_role          VARCHAR(100),
    email                 VARCHAR(200),
    created_at            TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- ORDER_FACT  (grain: 1 row per sales order)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ORDER_FACT (
    order_id                VARCHAR(30)    NOT NULL PRIMARY KEY,  -- SO-YYYY-NNNNNN
    purchase_order_id       VARCHAR(50),
    customer_account_id     VARCHAR(20)    NOT NULL REFERENCES CUSTOMER_DIM(customer_account_id),
    facility_id             VARCHAR(20)    NOT NULL REFERENCES FACILITY_DIM(facility_id),
    contact_id              VARCHAR(20)    REFERENCES CONTACT_DIM(contact_id),

    order_created_ts        TIMESTAMP_NTZ  NOT NULL,
    requested_ship_date     DATE,
    promised_delivery_date  DATE,
    actual_ship_ts          TIMESTAMP_NTZ,
    actual_delivery_date    DATE,

    status                  VARCHAR(20)    NOT NULL,
        -- CREATED | ALLOCATED | PICKED | SHIPPED | DELIVERED | BACKORDERED | CANCELLED | ON_HOLD
    status_last_updated_ts  TIMESTAMP_NTZ  NOT NULL,

    priority_flag           BOOLEAN        DEFAULT FALSE,
    carrier                 VARCHAR(50),
    tracking_number         VARCHAR(100),

    sales_region            VARCHAR(50),
    total_amount_usd        DECIMAL(14, 2),
    currency                VARCHAR(10)    DEFAULT 'USD',

    created_at              TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- ORDER_ITEM_FACT  (optional line-item detail)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ORDER_ITEM_FACT (
    order_item_id           VARCHAR(40)    NOT NULL PRIMARY KEY,
    order_id                VARCHAR(30)    NOT NULL REFERENCES ORDER_FACT(order_id),
    product_id              VARCHAR(20)    REFERENCES PRODUCT_DIM(product_id),
    quantity                INTEGER        DEFAULT 1,
    unit_price_usd          DECIMAL(12, 2),
    line_total_usd          DECIMAL(14, 2),
    created_at              TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
);

-- Useful clustering for date-range candidate queries
ALTER TABLE ORDER_FACT
    CLUSTER BY (order_created_ts::DATE, status);
