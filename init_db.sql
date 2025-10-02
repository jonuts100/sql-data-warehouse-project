/*
Script purpose:
  This script creates new db DataWarehouse if it already exists by first checking, then dropping and recreating.
  It also sets up bronze, silver, gold schemas within the db

Warning:
  If you already have a db named DataWarehouse with important data, change the db name in this script to
  avoid losing your data permanently. Keep backups before running this script ;)
*/

-- Create DATABASE 'DataWarehouse'

USE master;
GO

-- Drop and recreate DB DataWarehouse
IF EXISTS (SELECT 1 FROM sys.databases WHERE name='DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse
END;
GO

CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
