-- ==============================================================
-- BRONZE LAYER - Customers ingestion (Auto Loader via read_files)
-- Source : CSV files in UC volume /Volumes/demo2/bronze/data_files2
-- Target : demo2.bronze.customers (streaming table)
--
-- Raw, as-is ingestion:
--   * Schema inference + schema evolution (addNewColumns)
--   * Malformed / unexpected data is RESCUED into _rescued_data,
--     never silently discarded (warn-only check on customer_id)
--   * Ingestion metadata: ingestion_timestamp, source_file_name,
--     source_file_path
-- ==============================================================

CREATE OR REFRESH STREAMING TABLE demo2.bronze.customers (
  CONSTRAINT expect_customer_id EXPECT (customer_id IS NOT NULL)
)
COMMENT 'Bronze: raw customer CSVs ingested with Auto Loader from /Volumes/demo2/bronze/data_files2 (schema evolution + rescued data)'
AS SELECT
  *,
  current_timestamp()        AS ingestion_timestamp,
  _metadata.file_name       AS source_file_name,
  _metadata.file_path       AS source_file_path
FROM STREAM(read_files(
  '/Volumes/demo2/bronze/data_files2',
  format               => 'csv',
  header               => 'true',
  schemaEvolutionMode  => 'addNewColumns',
  rescuedDataColumn    => '_rescued_data',
  pathGlobFilter       => '*.csv'
));
