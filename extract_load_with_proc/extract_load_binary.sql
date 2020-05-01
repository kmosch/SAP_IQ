--extract binary
drop proc if exists dba.extract_data_binary;
create procedure dba.extract_data_binary( in owner varchar(256), in tbl varchar(256),in dir varchar(1024) )
begin
  declare str varchar(1024);
  set str = 'set temporary option Temp_Extract_Directory='+"char"(39)+dir+"char"(39)+';';
  execute immediate str;
  set str = 'set temporary option Temp_Extract_Name1='+"char"(39)++owner+'.'+tbl+'.txt'+"char"(39)+';';
  execute immediate with result set on str;
  set temporary option Temp_Extract_Binary = 'on';
  set str = 'set temporary option Temp_Extract_Directory='+"char"(39)+dir+"char"(39)+';';
  execute immediate str;
  set str = 'select * from '+owner+'.'+tbl+';';
  execute immediate with result set on str;
end;


-- unset options 
create or replace proc dba.unset_options()
begin
  set temporary option Temp_Extract_Name1 ='';
  set temporary option Temp_Extract_Directory='';
  set temporary option Temp_Extract_Binary = 'OFF';
end;

-- example for execution
call extract_data_binary('DBA','k','C:\Temp\k')
call unset_options();


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
create procedure DBA.write_load_binary( owner varchar(256),tbl varchar(256),iq_export_dir varchar(1024),filename varchar(1024),out result varchar(32000) )                                                                                                       
begin                                                                                                                                                                                                                                                            
  select 'LOAD TABLE '+Table_owner+'.'+Table_name+'('+col_list_binary(Table_owner,Table_name)+')'                                                                                                                                                                
    +'FROM '+"char"(39)+iq_export_dir+'\\'+filename+"char"(39)+"char"(10)                                                                                                                                                                                        
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
end                   

call write_load_binary('DBA','k','C:\Temp\k','DBA.Actions.txt')





-- load proc
create or replace proc load_table_binary(owner varchar(256),tbl varchar(256),iq_export_dir varchar(1024),filename varchar(1024), out res varchar(20000))
begin
declare @dt datetime;
message '---------------------------------------------------' to client;
message '---- Load Table '+owner+'.'+tbl+' --------' to client;
select 'truncate table '+Table_owner+'.'+Table_name+';'+
'LOAD TABLE '+Table_owner+'.'+Table_name+'('+col_list_binary(Table_owner,Table_name)+')'                                                                                                                                                                
    +'FROM '+"char"(39)+iq_export_dir+'\\'+filename+"char"(39)+"char"(10)                                                                                                                                                                                        
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
message 'Load Duration for '+owner+'.'+tbl+'(sec): '+convert(varchar(10), datediff(ss,@dt,getdate())) to client;
message '---------------------------------------------------' to client;
end;  

 
call load_table_binary('DBA','k','C:\Temp\k','DBA.Actions.txt')
