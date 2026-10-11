# 写入 Snowflake：MERGE 去重
def load_to_snowflake(table, target_schema, **context):
    s3_key = context["ti"].xcom_pull(task_ids=f"{table['name'].lower()}.extract")
    pk = table["primary_key"]
    on_clause = " AND ".join(f"t.{c} = s.{c}" for c in pk)

    sql = f"""
        -- 1. 把 parquet 读进临时表
        CREATE OR REPLACE TEMPORARY TABLE tmp_stage AS
        SELECT $1:* FROM @raw_stage/{s3_key} (FILE_FORMAT => 'parquet_fmt');

        -- 2. 回看窗口导致同一批里可能有同一主键的多个版本，只留最新的
        CREATE OR REPLACE TEMPORARY TABLE tmp_dedup AS
        SELECT * FROM tmp_stage
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY {', '.join(pk)}
            ORDER BY LAST_MODIFIED_TS DESC
        ) = 1;

        -- 3. MERGE：主键存在就更新，不存在就插入
        MERGE INTO {target_schema}.{table['name']} t
        USING tmp_dedup s
        ON {on_clause}
        WHEN MATCHED AND s.LAST_MODIFIED_TS >= t.LAST_MODIFIED_TS
            THEN UPDATE SET ...
        WHEN NOT MATCHED
            THEN INSERT ...;
    """
    SnowflakeHook("snowflake_default").run(sql)
