-- create_tables.sql
-- Drop tables if exists (để tránh lỗi khi chạy lại)
DROP TABLE IF EXISTS fact_view CASCADE;
DROP TABLE IF EXISTS dim_referrer_url CASCADE;
DROP TABLE IF EXISTS dim_collection CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_location CASCADE;
DROP TABLE IF EXISTS dim_user_agent CASCADE;
DROP TABLE IF EXISTS dim_store CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_user CASCADE;

-- Tạo các bảng dimension
CREATE TABLE dim_user (
    user_id TEXT PRIMARY KEY,
    device_id TEXT,
    email TEXT
);

CREATE TABLE dim_product (
    product_id TEXT PRIMARY KEY,
    product_name TEXT
);

CREATE TABLE dim_store (
    store_id TEXT PRIMARY KEY,
    store_name TEXT
);

CREATE TABLE dim_user_agent (
    user_agent_id BYTEA PRIMARY KEY,
    browser TEXT,
    os TEXT
);

CREATE TABLE dim_location (
    location_id BYTEA PRIMARY KEY,
    ip TEXT,
    country_short TEXT,
    country_long TEXT,
    region TEXT,
    city TEXT
);

CREATE TABLE dim_collection (
    collection_id BYTEA PRIMARY KEY,
    collection_name TEXT
);

CREATE TABLE dim_date (
    date_id TEXT PRIMARY KEY,
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
    referrer_url_id BYTEA PRIMARY KEY,
    referrer_url TEXT
);

-- Tạo bảng fact với foreign keys
CREATE TABLE fact_view (
    view_id TEXT PRIMARY KEY,
    product_id TEXT,
    referrer_url_id BYTEA,
    date_id TEXT,
    user_id TEXT,
    store_id TEXT,
    user_agent_id BYTEA,
    location_id BYTEA,
    current_url TEXT,
    api_version TEXT,
    collection_id BYTEA,
    
    -- Foreign Key constraints
    CONSTRAINT fk_fact_view_product 
        FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
    CONSTRAINT fk_fact_view_referrer_url 
        FOREIGN KEY (referrer_url_id) REFERENCES dim_referrer_url(referrer_url_id),
    CONSTRAINT fk_fact_view_date 
        FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    CONSTRAINT fk_fact_view_user 
        FOREIGN KEY (user_id) REFERENCES dim_user(user_id),
    CONSTRAINT fk_fact_view_store 
        FOREIGN KEY (store_id) REFERENCES dim_store(store_id),
    CONSTRAINT fk_fact_view_user_agent 
        FOREIGN KEY (user_agent_id) REFERENCES dim_user_agent(user_agent_id),
    CONSTRAINT fk_fact_view_location 
        FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
    CONSTRAINT fk_fact_view_collection 
        FOREIGN KEY (collection_id) REFERENCES dim_collection(collection_id)
);

-- Tạo indexes để tối ưu performance
CREATE INDEX idx_fact_view_product_id ON fact_view(product_id);
CREATE INDEX idx_fact_view_date_id ON fact_view(date_id);
CREATE INDEX idx_fact_view_user_id ON fact_view(user_id);
CREATE INDEX idx_fact_view_store_id ON fact_view(store_id);

-- Hiển thị kết quả
SELECT 'Tables created successfully!' as status;