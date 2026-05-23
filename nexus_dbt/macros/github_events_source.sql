{% macro github_events_source() %}
    s3(
        'https://nexus-iceberg.s3.eu-west-2.amazonaws.com/019e41af-5971-7972-a4d6-b12009ea5c42/019e41af-59ad-7e02-afe3-86294d417ab0/data/*/*.parquet',
        'Parquet'
    )
{% endmacro %}