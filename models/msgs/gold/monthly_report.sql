{{
    config(
        materialized = "table", 
        file_format = "delta",
        location_root = "/mnt/gold/msgs/monthly_report"
    )
}}

WITH messages_snapshot AS (
    SELECT
        signature,
        msg_id,
        channel_id,
        message,
        cleaned_message,
        date,
        msg_link,
        msg_from_peer,
        msg_from_id,
        views,
        number_replies,
        number_forwards,
        is_forward,
        is_reply,
        reply_to_msg_id,
        contains_media,
        media_type,
        url_id,
        document_id,
        poll_id,
        contact_userid,
        venue_id
    FROM {{ ref('messages_snapshot') }} 
    WHERE dbt_valid_to IS NULL
), 

channel_snapshot AS (
    SELECT
        channel_id,
        channel_name
    FROM {{ref('channel_snapshot')}} 
    WHERE dbt_valid_to IS NULL
), 

documents_snapshot as (
    SELECT
        document_id,
        document_type,
        document_video_duration,
        document_filename AS document_filename
    FROM {{ref('documents_snapshot')}}
    WHERE dbt_valid_to IS NULL
), 

transformed as (
    SELECT
        MONTH(date) AS month,
        YEAR(date) AS year,
        channel_name,
        document_type,
        COUNT(DISTINCT msg_id) AS n_msg,
        SUM(views) AS n_views,
        SUM(number_replies) AS n_replies
    FROM messages_snapshot mes
    INNER JOIN channel_snapshot cha on mes.channel_id = cha.channel_id
    INNER JOIN documents_snapshot doc on mes.document_id = doc.document_id
    GROUP BY 1,2,3,4
)
SELECT *
FROM transformed