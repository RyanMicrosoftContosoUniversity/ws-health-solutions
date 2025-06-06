CREATE OR ALTER PROCEDURE dbo.CaptureCountsAndRoles
    @ResultTable SYSNAME
AS
BEGIN
    SET NOCOUNT ON;

    -- Create result table if it does not exist
    DECLARE @create NVARCHAR(MAX) = N'
        IF OBJECT_ID(''' + @ResultTable + N''') IS NULL
        BEGIN
            CREATE TABLE ' + QUOTENAME(@ResultTable) + N'(
                ObjectType   NVARCHAR(20),
                SchemaName   SYSNAME NULL,
                ObjectName   SYSNAME NOT NULL,
                RowCount     BIGINT NULL,
                CaptureDate  DATETIME2 NOT NULL DEFAULT(SYSDATETIME())
            );
        END';
    EXEC(@create);

    -- Insert row counts for tables
    DECLARE @insert_tables NVARCHAR(MAX) = N'
        INSERT INTO ' + QUOTENAME(@ResultTable) + N' (ObjectType, SchemaName, ObjectName, RowCount)
        SELECT
            ''table'',
            s.name,
            t.name,
            SUM(p.rows)
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        JOIN sys.partitions p ON t.object_id = p.object_id
        WHERE p.index_id IN (0,1)
        GROUP BY s.name, t.name;';
    EXEC(@insert_tables);

    -- Insert user-defined roles
    DECLARE @insert_roles NVARCHAR(MAX) = N'
        INSERT INTO ' + QUOTENAME(@ResultTable) + N'(ObjectType, SchemaName, ObjectName, RowCount)
        SELECT ''role'', NULL, name, NULL
        FROM sys.database_principals
        WHERE type = ''R'' AND name NOT LIKE ''db_%'';';
    EXEC(@insert_roles);

    -- Insert row counts for views (WITHOUT CURSORS)
    DECLARE @sql NVARCHAR(MAX) = N'';
    SELECT @sql = STRING_AGG(
        'INSERT INTO ' + QUOTENAME(@ResultTable) + ' (ObjectType, SchemaName, ObjectName, RowCount)
         SELECT ''view'', ' +
         '''' + s.name + ''',' +
         '''' + v.name + ''',' +
         'COUNT(*) FROM ' + QUOTENAME(s.name) + '.' + QUOTENAME(v.name) + ';'
    , CHAR(13) + CHAR(10))
    FROM sys.views v
    JOIN sys.schemas s ON v.schema_id = s.schema_id;

    IF @sql IS NOT NULL AND LEN(@sql) > 0
        EXEC(@sql);
END
GO
