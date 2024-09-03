{% snapshot forward_snapshot %}

{{
    config(
      file_format = "delta",
      location_root = "/mnt/silver/msgs/forward",

      target_schema='snapshots',
      invalidate_hard_deletes=True,
      unique_key='msg_id',
      strategy='check',
      check_cols='all'
    )
}}

WITH source_data AS (
    SELECT DISTINCT
        msg_id,
        forward_msg_from_peer_type,
        forward_msg_from_peer_id,
        forward_msg_from_peer_name,
        CASE WHEN forward_msg_date = '-' THEN NULL 
             ELSE forward_msg_date
        END AS forward_msg_date,
        CASE WHEN forward_msg_date_string = '-' THEN NULL 
             ELSE forward_msg_date_string
        END AS forward_msg_date_string,
        forward_msg_link
    FROM {{ source('msgs', 'cop225') }}
    WHERE is_forward = 1
)
SELECT *
FROM source_data


{% endsnapshot %}