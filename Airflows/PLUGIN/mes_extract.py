# plugins/mes_extract.py 抽取函数：回看窗口在这里实现
from datetime import timedelta
import pendulum
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def extract_to_s3(conn_id, table, interval_start, interval_end, **context):
    start = pendulum.parse(interval_start)
    end = pendulum.parse(interval_end)

    # 回看：起点往前推 N 小时
    query_start = start - timedelta(hours=table["lookback_hours"])

    # MES 存的是工厂本地时间，查询条件也要转成本地时间
    query_start_local = query_start.in_timezone("America/Detroit").naive()
    end_local = end.in_timezone("America/Detroit").naive()

    sql = f"""
        SELECT *
        FROM dbo.{table['name']} WITH (NOLOCK)
        WHERE {table['incremental_column']} >  %s
          AND {table['incremental_column']} <= %s
    """
    hook = MsSqlHook(mssql_conn_id=conn_id)
    df = hook.get_pandas_df(sql, parameters=(query_start_local, end_local))

    # 加元数据，下游排查用
    df["_extracted_at"] = pendulum.now("UTC")
    df["_interval_start"] = start
    df["_interval_end"] = end

    # 按运行时间段存 parquet，路径确定 → 重跑会覆盖同一个文件
    key = f"mes/{table['name'].lower()}/{start.format('YYYY/MM/DD/HH')}/data.parquet"
    S3Hook().load_bytes(df.to_parquet(index=False), key=key,
                        bucket_name="battery-raw", replace=True)

    context["ti"].log.info(f"{table['name']}: {len(df)} rows "
                           f"[{query_start_local} → {end_local}]")
    return key
