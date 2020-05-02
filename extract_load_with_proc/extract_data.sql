drop proc if exists "dba"."col_list_load_text_export";
create or replace FUNCTION "DBA"."col_list_load_text_export" (@table_owner varchar(100),@table_name varchar(100), @delimiter varchar(4))
returns varchar(32000)
as
declare @column_list varchar(32000)
declare @table_id int ,
        @table_owner_id int,
        @col_num int ,
        @icount int ,
        @temp_col varchar(4000)
select @table_owner_id=user_id(@table_owner)
select @temp_col= NULL
select @column_list = NULL
select @icount= 1
select @table_id= (select table_id from SYSTABLE where table_name = @table_name and creator=@table_owner_id)
select @col_num = count(*) from SYSCOLUMN where table_id= @table_id
while @icount <= @col_num
begin
    select @temp_col = column_name from SYSCOLUMN where (table_id = @table_id) and (column_id = @icount)
    if @icount = @col_num
    begin
        select @column_list = @column_list + '['+ @temp_col + ']'+ char(10)
    end
    else
    begin
        select @column_list = @column_list + '['+ @temp_col + '] '+@delimiter+','+char(10)
    end
    select @icount = @icount + 1
end
return @column_list;

drop proc if exists "dba"."extract_data";
create or replace procedure "dba"."extract_data_text"( "owner" varchar(256),"tbl" varchar(256),"iq_export_dir" varchar(1024),"filename" varchar(1024),"col_delimiter" varchar(4),"row_delimiter" varchar(4), "result" varchar(30000) )
begin
  declare str varchar(1024);
 -- set temporary option "Timestamp_format" = 'YYYY-MM-DD HH:MM:SS';
  set str = 'set temporary option Temp_Extract_Directory='+"char"(39)+iq_export_dir+"char"(39)+';';
  execute immediate str;
  set str = 'set temporary option Temp_Extract_Name1='+"char"(39)+owner+'.'+tbl+'.txt'+"char"(39)+';';
  execute immediate with result set on str;
  set temporary option "Temp_Extract_append" = 'OFF';
  set str= 'set temporary option Temp_Extract_Column_Delimiter='+char(39)+col_delimiter+char(39);
    execute immediate with result set on str;
  set temporary option "Timestamp_format" = 'YYYY-MM-DD HH:MM:SS';
--  set temporary option "TEMP_EXTRACT_NULL_AS_EMPTY" = 'ON';
  set str = 'set temporary option Temp_Extract_Directory='+"char"(39)+iq_export_dir+"char"(39)+';';
  execute immediate str;
  set str = 'select * from '+owner+'.'+tbl+';';
  execute immediate with result set on str
end;
