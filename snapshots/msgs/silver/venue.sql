{% snapshot venue_snapshot %}

{{
    config(
      file_format = "delta",
      location_root = "/mnt/silver/msgs/venue",

      target_schema='snapshots',
      invalidate_hard_deletes=True,
      unique_key='venue_id',
      strategy='check',
      check_cols='all'
    )
}}

WITH source_data AS (
    SELECT DISTINCT
        venue_id AS venue_id,
        venue_type AS venue_type,
        venue_title AS venue_title,
        venue_address AS venue_address,
        venue_provider AS venue_provider,
        geo_type AS geo_type,
        lat AS lat,
        lng AS lng
    FROM {{ source('msgs', 'cop225') }}
)
SELECT *
FROM source_data

{% endsnapshot %}