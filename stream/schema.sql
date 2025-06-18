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

CREATE TABLE fact_view (
    view_id TEXT PRIMARY KEY,
    product_id TEXT REFERENCES dim_product(product_id),
    referrer_url_id BYTEA REFERENCES dim_referrer_url(referrer_url_id),
    date_id TEXT REFERENCES dim_date(date_id),
    user_id TEXT REFERENCES dim_user(user_id),
    store_id TEXT REFERENCES dim_store(store_id),
    user_agent_id BYTEA REFERENCES dim_user_agent(user_agent_id),
    location_id BYTEA REFERENCES dim_location(location_id),
    current_url TEXT,
    api_version TEXT,
    collection_id BYTEA REFERENCES dim_collection(collection_id)
);
