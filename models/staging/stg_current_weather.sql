SELECT
    -- Current weather data
    PARSE_JSON(_AIRBYTE_DATA):current:interval::NUMBER AS interval_seconds,
    PARSE_JSON(_AIRBYTE_DATA):current:temperature_2m::FLOAT AS current_temperature,
    PARSE_JSON(_AIRBYTE_DATA):current:wind_speed_10m::FLOAT AS current_wind_speed,
    PARSE_JSON(_AIRBYTE_DATA):current:time::TIMESTAMP AS current_observation_time,

    -- Location data
    PARSE_JSON(_AIRBYTE_DATA):elevation::FLOAT AS elevation,
    PARSE_JSON(_AIRBYTE_DATA):latitude::FLOAT AS latitude,
    PARSE_JSON(_AIRBYTE_DATA):longitude::FLOAT AS longitude,

    -- Hourly data (for a specific hour - example for first hour)
    PARSE_JSON(_AIRBYTE_DATA):hourly:relative_humidity_2m[0]::NUMBER AS hour_0_humidity,
    PARSE_JSON(_AIRBYTE_DATA):hourly:temperature_2m[0]::FLOAT AS hour_0_temperature,
    PARSE_JSON(_AIRBYTE_DATA):hourly:wind_speed_10m[0]::FLOAT AS hour_0_wind_speed,
    PARSE_JSON(_AIRBYTE_DATA):hourly:time[0]::TIMESTAMP AS hour_0_time,

    -- Metadata
    PARSE_JSON(_AIRBYTE_DATA):generationtime_ms::FLOAT AS generation_time_ms,
    CURRENT_TIMESTAMP() AS query_time

FROM WEATHER_DATA.RAW._AIRBYTE_RAW_PULL_FORECAST
