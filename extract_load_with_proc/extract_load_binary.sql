-- extract binary
-- in gui onteractive SQL it requires both "Show reults foa all statements" and "Show all result sets"
drop proc if exists dba.extract_data_binary;
create or replace procedure dba.extract_data_binary( in owner varchar(256), in tbl varchar(256),in dir varchar(1024) )
begin
  declare str varchar(1024);
  declare @dt datetime;
  declare cnt bigint;
  execute immediate 'select count(*) into cnt from '+owner+'.'+tbl+';';
  message '---------------------------------------------------' to client;
  message '-- Extract Table '+owner+'.'+tbl to client;
  message '-- Number or rows to be extracted for '+owner+'.'+tbl+' : '+convert(varchar(20),cnt) to client;
  set @dt=getdate();
  set str = 'set temporary option Temp_Extract_Directory='+"char"(39)+dir+"char"(39)+';';
  execute immediate str;
  set str = 'set temporary option Temp_Extract_Name1='+"char"(39)++owner+'.'+tbl+'.txt'+"char"(39)+';';
  execute immediate with result set on str;
  set temporary option Temp_Extract_Binary = 'on';
  set str = 'set temporary option Temp_Extract_Directory='+"char"(39)+dir+"char"(39)+';';
  execute immediate str;
  set str = 'select * from '+owner+'.'+tbl+';';
  execute immediate with result set on str;
  set temporary option Temp_Extract_Name1 ='';
  set temporary option Temp_Extract_Directory='';
  set temporary option Temp_Extract_Binary = 'OFF';
  message '-- Extraction Duration for '+owner+'.'+tbl+'(sec): '+convert(varchar(10), datediff(ss,@dt,getdate())) to client;
  message '---------------------------------------------------' to client;
end;

-- example for execution
call extract_data_binary('DBA','k','C:\Temp\k')

--column list for binary load 
drop proc if exists "DBA"."col_list_binary";
create FUNCTION "dba"."col_list_binary" (@table_owner varchar(100),@table_name varchar(100))
returns varchar(32000)    
as
      declare @column_list varchar(32000),  
              @table_id   int,
			  @table_owner_id int,
              @col_num    int,
              @icount       int,
              @temp_col varchar(4000)  
     
        select @table_owner_id=user_id(@table_owner)      
        select @temp_col    = NULL
        select @column_list = NULL
        select @icount      = 1
        select @table_id    =  table_id from SYSTABLE where table_name = @table_name and creator=@table_owner_id    
        select @col_num = count(*) from  SYSCOLUMN where table_id= @table_id
        while @icount <= @col_num
              begin
                    select @temp_col = column_name 
                    from SYSCOLUMN 
                    where (table_id = @table_id) and (column_id = @icount)
					if @icount = @col_num
                    begin
                        select @column_list = @column_list + '['+ @temp_col + ']' + ' BINARY WITH NULL BYTE'+ char(10)
                    end
					else
                    begin
                        select @column_list = @column_list + '['+ @temp_col + ']'+ ' BINARY WITH NULL BYTE,'  + char(10)
                    end
                    select @icount = @icount + 1
              end
return @column_list;




-- prints load command
create or replace procedure DBA.write_load_binary( owner varchar(256),tbl varchar(256),iq_export_dir varchar(1024),filename varchar(1024),out result varchar(32000) )                                                                                                       
begin    
  DECLARE path_delimiter CHAR(1) ;
  DECLARE platform varchar(20) ;
  SELECT Value INTO platform from  sa_eng_properties() where PropName='Platform';
  select   CASE left(lower(platform),3)
  WHEN 'win' THEN  '\'
  ELSE '/'
  END into path_delimiter;                                                                                                                                                                                                                                                   
  select 'LOAD TABLE '+Table_owner+'.'+Table_name+'('+col_list_binary(Table_owner,Table_name)+')'                                                                                                                                                                
    +'FROM '+"char"(39)+iq_export_dir+path_delimiter+filename+"char"(39)+"char"(10)                                                                                                                                                                                        
    +'format binary'+"char"(10)                                                                                                                                                                                                                                  
    +'on file error rollback'+"char"(10)                                                                                                                                                                                                                         
    +'notify 1000000'+"char"(10)                                                                                                                                                                                                                                 
    +'strip off'+"char"(10)                                                                                                                                                                                                                                      
    +'quotes off'+"char"(10)                                                                                                                                                                                                                                     
    +'escapes off'+"char"(10)                                                                                                                                                                                                                                    
    +'preview on;'+"char"(10)                                                                                                                                                                                                                                    
    +'commit;'                                                                                                                                                                                                                                                   
    into result from sp_iqtable()                                                                                                                                                                                                                                
    where Table_owner = owner                                                                                                                                                                                                                                    
    and Table_name = tbl;                                                                                                                                                                                                                                        
  select result                                                                                                                                                                                                                                                  
end;                   

call write_load_binary('DBA','k','C:\Temp\k','DBA.Actions.txt');





-- load proc
create or replace proc load_table_binary(owner varchar(256),tbl varchar(256),iq_export_dir varchar(1024),filename varchar(1024), out res varchar(20000))
begin
declare @dt datetime;
declare cnt int;
DECLARE path_delimiter CHAR(1) ;
DECLARE platform varchar(20) ;
SELECT Value INTO platform from  sa_eng_properties() where PropName='Platform';
select   CASE left(lower(platform),3)
WHEN 'win' THEN  '\'
ELSE '/'
END into path_delimiter;   
message '---------------------------------------------------' to client;
message '-- Load Table '+owner+'.'+tbl to client;
select 'truncate table '+Table_owner+'.'+Table_name+';'+
'LOAD TABLE '+Table_owner+'.'+Table_name+'('+col_list_binary(Table_owner,Table_name)+')'                                                                                                                                                                
    +'FROM '+"char"(39)+iq_export_dir+path_delimiter+filename+"char"(39)+"char"(10)                                                                                                                                                                                        
    +'format binary'+"char"(10)                                                                                                                                                                                                                                  
    +'on file error rollback'+"char"(10)                                                                                                                                                                                                                         
    +'notify 1000000'+"char"(10)                                                                                                                                                                                                                                 
    +'strip off'+"char"(10)                                                                                                                                                                                                                                      
    +'quotes off'+"char"(10)                                                                                                                                                                                                                                     
    +'escapes off'+"char"(10)                                                                                                                                                                                                                                    
    +'preview on;'+"char"(10)                                                                                                                                                                                                                                    
    +'commit;'    into res
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

 
call load_table_binary('DBA','k','C:\Temp\k','DBA.Actions.txt');




