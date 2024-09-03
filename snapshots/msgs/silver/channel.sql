{% snapshot channel_snapshot %}

{{
    config(
      file_format = "delta",
      location_root = "/mnt/silver/msgs/channel",

      target_schema='snapshots',
      invalidate_hard_deletes=True,
      unique_key='channel_id',
      strategy='check',
      check_cols='all'
    )
}}

WITH source_data AS (
    SELECT DISTINCT
       channel_id AS channel_id,
       channel_name AS channel_name
    FROM {{ source('msgs', 'cop225') }}
)
SELECT *
FROM source_data

{% endsnapshot %}