/*
===============================================================================
Database Exploration
===============================================================================
Purpose:
    - Explore the structure of the database, including schemas and tables.
    - Inspect columns and metadata for specific tables.

System Views Used:
    - information_schema.tables
    - information_schema.columns
===============================================================================
*/

-- =====================================================
-- Retrieve a list of all user tables in the database
-- =====================================================

SELECT 
    table_catalog,
    table_schema,
    table_name,
    table_type
FROM information_schema.tables
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY table_schema, table_name;


-- =====================================================
-- Retrieve all columns for a specific table (dim_customers)
-- =====================================================

SELECT 
    column_name,
    data_type,
    is_nullable,
    character_maximum_length
FROM information_schema.columns
WHERE table_name = 'dim_customers'
  AND table_schema = 'gold'
ORDER BY ordinal_position;
