{{ config(materialized='view') }}

with source as (

    select * from {{ source('mes', 'fill_station_result') }}

),

cleaned as (

    select
        -- 标识
        {{ dbt_utils.generate_surrogate_key(['CELL_BC', 'STN_ID', 'FILL_TS']) }}
                                                    as fill_record_id,
        trim(upper(CELL_BC))                        as cell_id,
        trim(STN_ID)                                as station_id,
        trim(LINE_ID)                               as line_id,
        trim(PLANT_CD)                              as plant_code,

        -- 时间：MES 存的是工厂本地时间，没有时区信息，统一转 UTC
        convert_timezone(
            'America/Detroit', 'UTC', FILL_TS::timestamp_ntz
        )                                           as fill_ts_utc,

        -- 重量：老设备上报 kg，新设备上报 g，用 WT_UNIT 字段区分，统一成 g
        case upper(WT_UNIT)
            when 'KG' then FL_WT_BEF::float * 1000
            else FL_WT_BEF::float
        end                                         as weight_before_g,

        case upper(WT_UNIT)
            when 'KG' then FL_WT_AFT::float * 1000
            else FL_WT_AFT::float
        end                                         as weight_after_g,

        case upper(WT_UNIT)
            when 'KG' then FL_WT_TGT::float * 1000
            else FL_WT_TGT::float
        end                                         as fill_target_g,

        -- 结果
        upper(trim(RSLT)) = 'OK'                    as is_pass,
        coalesce(RETRY_FLG, 0) = 1                  as is_retry,

        -- 元数据
        _loaded_at,
        _source_file

    from source
    where CELL_BC is not null
      and trim(CELL_BC) != ''

)

select
    *,
    weight_after_g - weight_before_g as fill_weight_g
from cleaned
