{% snapshot documents_snapshot %}

{{
    config(
      file_format = "delta",
      location_root = "/mnt/silver/msgs/documents",

      target_schema='snapshots',
      invalidate_hard_deletes=True,
      unique_key='document_id',
      strategy='check',
      check_cols='all'
    )
}}

WITH source_data AS (
    SELECT DISTINCT
        document_id AS document_id,
        document_type AS document_type,
        ROUND(document_video_duration) AS document_video_duration,
        document_filename AS document_filename
    FROM {{ source('msgs', 'cop225') }}
)
SELECT *
FROM source_data

{% endsnapshot %}