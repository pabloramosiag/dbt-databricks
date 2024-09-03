{% snapshot contact_snapshot %}

{{
    config(
      file_format = "delta",
      location_root = "/mnt/silver/msgs/contact",

      target_schema='snapshots',
      invalidate_hard_deletes=True,
      unique_key='contact_userid',
      strategy='check',
      check_cols='all'
    )
}}

WITH source_data AS (
    SELECT DISTINCT
        contact_userid AS contact_userid,
        contact_phone_number AS contact_phone_number,
        contact_name AS contact_name
    FROM {{ source('msgs', 'cop225') }}
)
SELECT *
FROM source_data


{% endsnapshot %}