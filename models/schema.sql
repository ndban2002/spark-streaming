-- recreate_tables_text_ids.sql
DROP TABLE IF EXISTS fact_view CASCADE;
DROP TABLE IF EXISTS dim_referrer_url CASCADE;
DROP TABLE IF EXISTS dim_collection CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_location CASCADE;
DROP TABLE IF EXISTS dim_user_agent CASCADE;
DROP TABLE IF EXISTS dim_store CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_user CASCADE;

-- Tạo lại bảng với tất cả ID là TEXT
CREATE TABLE dim_user (
    user_id TEXT,
    device_id TEXT,
    email TEXT
);

CREATE TABLE dim_product (
    product_id TEXT,
    product_name TEXT
);

CREATE TABLE dim_store (
    store_id TEXT,
    store_name TEXT
);

CREATE TABLE dim_user_agent (
    user_agent_id TEXT,  -- Đổi từ BYTEA thành TEXT
    browser TEXT,
    os TEXT
);

CREATE TABLE dim_location (
    location_id TEXT,    -- Đổi từ BYTEA thành TEXT
    ip TEXT,
    country_short TEXT,
    country_long TEXT,
    region TEXT,
    city TEXT
);

CREATE TABLE dim_collection (
    collection_id TEXT,  -- Đổi từ BYTEA thành TEXT
    collection_name TEXT
);

CREATE TABLE dim_date (
    date_id TEXT,
    hour INTEGER,
    day_of_week TEXT,
    day_of_week_short TEXT,
    month TEXT,
    year_month TEXT,
    year INTEGER,
    year_number INTEGER,
    is_weekday_or_weekend TEXT
);

CREATE TABLE dim_referrer_url (
    referrer_url_id TEXT,  -- Đổi từ BYTEA thành TEXT
    referrer_url TEXT
);

CREATE TABLE fact_view (
    view_id TEXT,
    product_id TEXT,
    referrer_url_id TEXT,     -- Đổi từ BYTEA thành TEXT
    date_id TEXT,
    user_id TEXT,
    store_id TEXT,
    user_agent_id TEXT,       -- Đổi từ BYTEA thành TEXT
    location_id TEXT,         -- Đổi từ BYTEA thành TEXT
    current_url TEXT,
    api_version TEXT,
    collection_id TEXT        -- Đổi từ BYTEA thành TEXT
);

-- Tạo indexes
CREATE INDEX idx_fact_view_product_id ON fact_view(product_id);
CREATE INDEX idx_fact_view_date_id ON fact_view(date_id);
CREATE INDEX idx_fact_view_user_id ON fact_view(user_id);
CREATE INDEX idx_fact_view_store_id ON fact_view(store_id);

SELECT 'Tables recreated with TEXT IDs!' as status;