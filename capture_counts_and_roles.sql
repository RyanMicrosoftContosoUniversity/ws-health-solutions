-- Stored procedure to capture row counts for all tables and views
-- and list user-defined database roles. The results are written
-- to the table specified by @ResultTable.

CREATE OR ALTER PROCEDURE dbo.CaptureCountsAndRoles
    @ResultTable SYSNAME
AS
BEGIN
    SET NOCOUNT ON;

    -- Create result table if it does not exist
    IF OBJECT_ID(@ResultTable) IS NULL
    BEGIN
        DECLARE @create NVARCHAR(MAX) = N'CREATE TABLE ' + QUOTENAME(@ResultTable) + N'(
            ObjectType   NVARCHAR(20),
            SchemaName   SYSNAME NULL,
            ObjectName   SYSNAME NOT NULL,
            RowCount     BIGINT NULL,
            CaptureDate  DATETIME2 NOT NULL DEFAULT(SYSDATETIME())
        );';
        EXEC(@create);
    END

    DECLARE @schema SYSNAME;
    DECLARE @name SYSNAME;
    DECLARE @sql NVARCHAR(MAX);

    -- Collect table counts
    DECLARE table_cur CURSOR FOR
        SELECT s.name, t.name
        FROM sys.tables AS t
        JOIN sys.schemas AS s ON t.schema_id = s.schema_id;
    OPEN table_cur;
    FETCH NEXT FROM table_cur INTO @schema, @name;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @sql = N'INSERT INTO ' + QUOTENAME(@ResultTable) + N'(ObjectType, SchemaName, ObjectName, RowCount)
            SELECT ''table'', @schema, @name, COUNT(*) FROM ' + QUOTENAME(@schema) + N'.' + QUOTENAME(@name) + N';';
        EXEC sp_executesql @sql, N'@schema SYSNAME, @name SYSNAME', @schema=@schema, @name=@name;
        FETCH NEXT FROM table_cur INTO @schema, @name;
    END
    CLOSE table_cur;
    DEALLOCATE table_cur;

    -- Collect view counts
    DECLARE view_cur CURSOR FOR
        SELECT s.name, v.name
        FROM sys.views AS v
        JOIN sys.schemas AS s ON v.schema_id = s.schema_id;
    OPEN view_cur;
    FETCH NEXT FROM view_cur INTO @schema, @name;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @sql = N'INSERT INTO ' + QUOTENAME(@ResultTable) + N'(ObjectType, SchemaName, ObjectName, RowCount)
            SELECT ''view'', @schema, @name, COUNT(*) FROM ' + QUOTENAME(@schema) + N'.' + QUOTENAME(@name) + N';';
        EXEC sp_executesql @sql, N'@schema SYSNAME, @name SYSNAME', @schema=@schema, @name=@name;
        FETCH NEXT FROM view_cur INTO @schema, @name;
    END
    CLOSE view_cur;
    DEALLOCATE view_cur;

    -- Insert database roles
    SET @sql = N'INSERT INTO ' + QUOTENAME(@ResultTable) + N'(ObjectType, SchemaName, ObjectName, RowCount)
        SELECT ''role'', NULL, name, NULL
        FROM sys.database_principals
        WHERE type = ''R'' AND name NOT LIKE ''db_%'';';
    EXEC(@sql);
END
GO
