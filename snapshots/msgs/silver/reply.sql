{% snapshot reply_snapshot %}

{{
    config(
      file_format = "delta",
      location_root = "/mnt/silver/msgs/reply",

      target_schema='snapshots',
      invalidate_hard_deletes=True,
      unique_key='msg_id||'-'||reply_to_msg_id',
      strategy='check',
      check_cols='all'
    )
}}

WITH source_data AS (
    SELECT DISTINCT
        msg_id AS msg_id,
        reply_to_msg_id AS reply_to_msg_id,
        reply_msg_link AS reply_msg_link
    FROM {{ source('msgs', 'cop225') }}
)
SELECT *
FROM source_data



{% endsnapshot %}