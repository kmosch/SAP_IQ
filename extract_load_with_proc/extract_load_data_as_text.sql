drop proc if exists dba.extract_data_text;
create or replace procedure dba.extract_data_text( owner varchar(256),tbl varchar(256),iq_export_dir varchar(1024),col_delimiter varchar(4),row_delimiter varchar(4) )
begin
  declare str varchar(1024);
  declare @dt datetime;
  declare cnt bigint;
  execute immediate 'select count(*) into cnt from '+owner+'.'+tbl+';';
  message '---------------------------------------------------' to client;
  message '-- Extract Table '+owner+'.'+tbl to client;
  message '-- Number or rows to be extracted for '+owner+'.'+tbl+' : '+convert(varchar(20),cnt) to client;
  set @dt=getdate();
  set temporary option Temp_Extract_append = 'OFF';
  set temporary option Temp_Extract_Binary = 'OFF';
-- set temporary option Timestamp_format = 'YYYY-MM-DD HH:MM:SS';
-- set temporary option TEMP_EXTRACT_NULL_AS_EMPTY = 'ON';
-- set temporary option Timestamp_format = 'YYYY-MM-DD HH:MM:SS';
  set str = 'set temporary option Temp_Extract_Directory='+char(39)+iq_export_dir+char(39)+';';
  execute immediate str;
  set str = 'set temporary option Temp_Extract_Name1='+char(39)+owner+'.'+tbl+'.txt'+char(39)+';';
  execute immediate with result set on str;
  set str= 'set temporary option Temp_Extract_Column_Delimiter='+char(39)+col_delimiter+char(39);
  execute immediate with result set on str;
  set str= 'set temporary option Temp_Extract_Row_Delimiter='+char(39)+row_delimiter+char(39);
  execute immediate with result set on str;
  set str = 'set temporary option Temp_Extract_Directory='+char(39)+iq_export_dir+char(39)+';';
  execute immediate str;
  set str = 'select * from '+owner+'.'+tbl+';';
  execute immediate with result set on str;
  set temporary option Temp_Extract_Name1 ='';
  set temporary option Temp_Extract_Directory='';
  message '-- Extraction Duration for '+owner+'.'+tbl+'(sec): '+convert(varchar(10), datediff(ss,@dt,getdate())) to client;
  message '---------------------------------------------------' to client;
end;

-- call dba.extract_data_text('dba','STG_cr__comast','/sybdata/iq/kiq/export','|#|','!@#')

-- function to create the column list for the load command
drop proc if exists dba.col_list_load_text_export;
create or replace FUNCTION DBA.col_list_load_text_export (@table_owner varchar(100),@table_name varchar(100),@delimiter varchar(4))
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
        select @column_list = @column_list + '['+ @temp_col + '] '+char(39)+@delimiter+char(39)+' null(''NULL'')'+char(10)
    end
    else
    begin
        select @column_list = @column_list + '['+ @temp_col + '] '+char(39)+@delimiter+char(39)+' null(''NULL'')'+','+char(10)
    end
    select @icount = @icount + 1
end
return @column_list;

-- select col_list_load_text_export('dba','STG_cr__comast','|#|')

-- pront the load command
create or replace procedure DBA.write_load_text_export(owner varchar(256),tbl varchar(256),iq_export_dir varchar(1024),filename varchar(1024),col_delimiter varchar(4),row_delimiter varchar(4),out result varchar(10000) )
begin
DECLARE path_delimiter CHAR(1) ;
DECLARE platform varchar(20) ;
SELECT Value INTO platform from  sa_eng_properties() where PropName='Platform';
select   CASE left(lower(platform),3)
          WHEN 'win' THEN  '\'
          ELSE '/'
        END into path_delimiter; 
select 'LOAD TABLE '+Table_owner+'.'+Table_name+'('+col_list_load_text_export(Table_owner,Table_name,col_delimiter)+')' 
+'FROM '+char(39)+iq_export_dir+path_delimiter+filename+char(39)+char(10)
+'row delimited by '+char(39)+row_delimiter+char(39)+char(10)
+'on file error rollback'+char(10) 
+'notify 1000000'+char(10) 
+'quotes off'+char(10) 
+'escapes off;'+char(10) 
+'commit;' 
into result
from sp_iqtable()
where Table_owner = owner
and Table_name = tbl;
select result
end;

-- call write_load_text_export('dba','STG_cr__comast','/sybdata/iq/kiq/export', 'dba.STG_cr__comast.txt','|#|','!@#')

-- load proc
create or replace proc load_table_text_export(owner varchar(256),tbl varchar(256),iq_export_dir varchar(1024),filename varchar(1024),col_delimiter varchar(4),row_delimiter varchar(4))
begin
    declare res varchar(32000);
    declare @dt datetime;
    declare cnt int;
    declare path_delimiter CHAR(1) ;
    declare platform varchar(20) ;
    SELECT Value INTO platform from  sa_eng_properties() where PropName='Platform';
    select   CASE left(lower(platform),3)
    WHEN 'win' THEN  '\'
    ELSE '/'
    END into path_delimiter;   
    message '---------------------------------------------------' to client;
    message '-- Load Table '+owner+'.'+tbl to client;
    select 'truncate table '+Table_owner+'.'+Table_name+';'
          +'LOAD TABLE '+Table_owner+'.'+Table_name+'('+col_list_load_text_export(Table_owner,Table_name,col_delimiter)+')' 
          +'FROM '+char(39)+iq_export_dir+path_delimiter+filename+char(39)+char(10)
          +'row delimited by '+char(39)+row_delimiter+char(39)+char(10)
          +'on file error rollback'+char(10) 
          +'notify 1000000'+char(10) 
          +'quotes off'+char(10) 
          +'escapes off;'+char(10) 
          +'commit;' into res
    from sp_iqtable()  
    where  Table_owner=owner and 
    Table_name =tbl;
    set @dt=getdate();
    execute immediate  res;
    set cnt=@@rowcount;
    message '-- Number of rows loaded in '+owner+'.'+tbl+': '+convert(varchar(30), cnt) to client;
    message '-- Load Duration for '+owner+'.'+tbl+'(sec): '+convert(varchar(10), datediff(ss,@dt,getdate())) to client;
    message '---------------------------------------------------' to client;
end;  

 
-- call load_table_text_export('dba','STG_cr__comast','/sybdata/iq/kiq/export', 'dba.STG_cr__comast.txt','|#|','!@#');