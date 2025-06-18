def write_to_postgres(df, table_name, config, mode="ignore"):
    url = f"jdbc:postgresql://{config['host']}:{config['port']}/{config['database']}"
    df.write \
        .mode(mode) \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .option("user", config['user']) \
        .option("password", config['password']) \
        .option("driver", "org.postgresql.Driver") \
        .save()
