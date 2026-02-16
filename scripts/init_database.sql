/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'data_warehouse' after checking if it already exists.
    If the database exists, it is dropped and recreated.
    Additionally, the script sets up three schemas within the database:
    'bronze', 'silver', and 'gold'

WARNING:
    Running this script will drop the entire 'data_warehouse' database if it exists.
    All data in the database will be permanently deleted.

=============================================================
*/

-- Connect to default database first
\c postgres;

-- Terminate active connections to the database
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'data_warehouse'
  AND pid <> pg_backend_pid();

-- Drop database if exists
DROP DATABASE IF EXISTS data_warehouse;

-- Create database
CREATE DATABASE data_warehouse;

-- ==========================================
-- Connect to the new database before continuing
-- ==========================================
\c data_warehouse;

-- Create Schemas
CREATE SCHEMA bronze;

CREATE SCHEMA silver;

CREATE SCHEMA gold;
