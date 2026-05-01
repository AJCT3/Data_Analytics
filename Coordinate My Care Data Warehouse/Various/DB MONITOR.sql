create table #dbfileInfo(
name varchar(300),
location varchar(300),
filesizeMB decimal(9,2),
spaceUsedMB decimal(9,2),
FreespaceMB decimal(9,2))

declare @mySQL nvarchar(2000)
DECLARE @dbName varchar(MAX)
DECLARE @cur_DBName CURSOR

SET @cur_DBName = CURSOR FOR
select name from sys.databases

OPEN @cur_DBName
FETCH NEXT
FROM @cur_DBName INTO @dbName
WHILE @@FETCH_STATUS = 0
BEGIN
PRINT @dbName
if DATABASEPROPERTYEX(@dbName, 'status') = 'ONLINE'
begin
select @mySQL = 
    '
        use ' + @dbname + '
        INSERT INTO #dbfileInfo
        select
      name
    , filename
    , convert(decimal(12,2),round(a.size/128.000,2)) as FileSizeMB
    , convert(decimal(12,2),round(fileproperty(a.name,''SpaceUsed'')/128.000,2)) as SpaceUsedMB
    , convert(decimal(12,2),round((a.size-fileproperty(a.name,''SpaceUsed''))/128.000,2)) as FreeSpaceMB
    from dbo.sysfiles a
    '
    exec sp_executesql @mySQL
end
FETCH NEXT
FROM @cur_DBName INTO @dbName

END
CLOSE @cur_DBName
DEALLOCATE @cur_DBName
GO

select * from #dbfileInfo
drop table #dbfileInfo
