{{
    config(
        materialized='incremental',
        unique_key='fill_record_id',
        incremental_strategy='merge',
        cluster_by=['to_date(fill_ts_utc)', 'line_id']
    )
}}

with fills as (

    select * from {{ ref('stg_mes__fill_station') }}

    {% if is_incremental() %}
    -- 只处理新数据；但同一颗电芯的重注可能隔几小时才发生，
    -- 所以要把"有新记录的电芯"的全部历史记录都拿出来重新编号
    where cell_id in (
        select distinct cell_id
        from {{ ref('stg_mes__fill_station') }}
        where _loaded_at > (select max(_loaded_at) from {{ this }})
    )
    {% endif %}

),

numbered as (

    select
        *,
        row_number() over (
            partition by cell_id
            order by fill_ts_utc, fill_record_id   -- 时间相同时用 id 保证顺序稳定
        )                                           as fill_attempt_seq,

        count(*) over (partition by cell_id)        as total_fill_attempts

    from fills

),

final as (

    select
        fill_record_id,
        cell_id,
        station_id,
        line_id,
        plant_code,
        fill_ts_utc,
        fill_attempt_seq,
        total_fill_attempts,
        fill_attempt_seq = total_fill_attempts      as is_final_attempt,

        weight_before_g,
        weight_after_g,
        fill_weight_g,
        fill_target_g,

        -- 偏差百分比；目标值为 0 或空时返回 null，避免除零
        round(
            (fill_weight_g - fill_target_g)
            / nullif(fill_target_g, 0) * 100,
            3
        )                                           as fill_deviation_pct,

        is_pass,
        is_retry,
        _loaded_at

    from numbered

)

select * from final
