{% snapshot messages_snapshot %}

{{
    config(
      file_format = "delta",
      location_root = "/mnt/silver/msgs/messages",

      target_schema='snapshots',
      invalidate_hard_deletes=True,
      unique_key='signature',
      strategy='check',
      check_cols='all'
    )
}}

WITH source_data AS (
    SELECT DISTINCT
        signature AS signature,
        msg_id AS msg_id,
        channel_id AS channel_id,
        LOWER(message) AS message,
        LOWER(cleaned_message) AS cleaned_message,
        date AS date,
        msg_link AS msg_link,
        msg_from_peer AS msg_from_peer,
        msg_from_id AS msg_from_id,
        views AS views,
        number_replies AS number_replies,
        number_forwards AS number_forwards,
        is_forward AS is_forward,
        is_reply AS is_reply,
        reply_to_msg_id AS reply_to_msg_id,
        contains_media AS contains_media,
        REPLACE(media_type,'MessageMedia','') AS media_type,
        hash(url) AS url_id,
        document_id AS document_id,
        poll_id AS poll_id,
        contact_userid AS contact_userid,
        venue_id AS venue_id
    FROM {{ source('msgs', 'cop225') }}
)
SELECT *
FROM source_data

{% endsnapshot %}