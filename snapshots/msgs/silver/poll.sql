{% snapshot poll_snapshot %}

{{
    config(
      file_format = "delta",
      location_root = "/mnt/silver/msgs/poll",

      target_schema='snapshots',
      invalidate_hard_deletes=True,
      unique_key='poll_id',
      strategy='check',
      check_cols='all'
    )
}}

WITH source_data AS (
    SELECT DISTINCT
        poll_id AS poll_id,
        poll_question AS poll_question,
        poll_total_voters AS poll_total_voters,
        JSON_UNQUOTE(JSON_EXTRACT({{ transform_poll_results('poll_results') }}, '$[0].option')) AS option_1,
        JSON_UNQUOTE(JSON_EXTRACT({{ transform_poll_results('poll_results') }}, '$[0].voters')) AS voters_1,
        JSON_UNQUOTE(JSON_EXTRACT({{ transform_poll_results('poll_results') }}, '$[0].chosen')) AS chosen_1,
        JSON_UNQUOTE(JSON_EXTRACT({{ transform_poll_results('poll_results') }}, '$[0].correct')) AS correct_1,
        JSON_UNQUOTE(JSON_EXTRACT({{ transform_poll_results('poll_results') }}, '$[1].option')) AS option_2,
        JSON_UNQUOTE(JSON_EXTRACT({{ transform_poll_results('poll_results') }}, '$[1].voters')) AS voters_2,
        JSON_UNQUOTE(JSON_EXTRACT({{ transform_poll_results('poll_results') }}, '$[1].chosen')) AS chosen_2,
        JSON_UNQUOTE(JSON_EXTRACT({{ transform_poll_results('poll_results') }}, '$[1].correct')) AS correct_2,
        JSON_UNQUOTE(JSON_EXTRACT({{ transform_poll_results('poll_results') }}, '$[2].option')) AS option_3,
        JSON_UNQUOTE(JSON_EXTRACT({{ transform_poll_results('poll_results') }}, '$[2].voters')) AS voters_3,
        JSON_UNQUOTE(JSON_EXTRACT({{ transform_poll_results('poll_results') }}, '$[2].chosen')) AS chosen_3,
        JSON_UNQUOTE(JSON_EXTRACT({{ transform_poll_results('poll_results') }}, '$[2].correct')) AS correct_3
    FROM {{ source('msgs', 'cop225') }}
)
SELECT *
FROM source_data

{% endsnapshot %}