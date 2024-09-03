{% snapshot url_snapshot %}

{{
    config(
      file_format = "delta",
      location_root = "/mnt/silver/msgs/url",

      target_schema='snapshots',
      invalidate_hard_deletes=True,
      unique_key='url_id',
      strategy='check',
      check_cols='all'
    )
}}

WITH source_data AS (
    SELECT DISTINCT
        hash(url) AS url_id,
        has_url AS has_url,
        url AS url,
        domain AS domain,
        url_title AS url_title,
        CASE WHEN url_description = '...' THEN NULL
             ELSE url_description 
        END AS url_description
    FROM {{ source('msgs', 'cop225') }}
)
SELECT *
FROM source_data

{% endsnapshot %}