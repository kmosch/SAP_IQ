﻿--extract bcp file from ASE this means that the last column has both delimiters


drop proc if exists "DBA"."col_list_bcp";
create FUNCTION "DBA"."col_list_bcp" (@table_owner varchar(100),@table_name varchar(100))
returns varchar(32000)
as
declare @column_list varchar(32000),
@table_id int,
			@table_owner_id int,
@col_numint,
@icount int,
@temp_col varchar(4000)
select @table_owner_id=user_id(@table_owner)
select @temp_col= NULL
select @column_list = NULL
select @icount= 1
select @table_id=table_id from SYSTABLE where table_name = @table_name and creator=@table_owner_id
select @col_num = count(*) from SYSCOLUMN where table_id= @table_id
while @icount <= @col_num
begin
select @temp_col = column_name from SYSCOLUMN where (table_id = @table_id) and (column_id = @icount)
if @icount = @col_num
begin
  select @column_list = @column_list + '['+ @temp_col + ']' +char(10)
end
else
begin
  select @column_list = @column_list + '['+ @temp_col + ']' + ','+ char(10)
end
select @icount = @icount + 1
end

return @column_list;

 
 
-- prints load command
create or replace procedure "DBA"."write_load"( "owner" varchar(256),"tbl" varchar(256),"iq_export_dir" varchar(1024),"filename" varchar(1024),"col_delimiter" varchar(4),"row_delimiter" varchar(4),out "result" varchar(10000) )
begin
select 'LOAD TABLE '+"Table_owner"+'.'+"Table_name"+'('+"col_list_bcp"("Table_owner","Table_name")+')' 
+'FROM '+"char"(39)+"iq_export_dir"+'\\'+"filename"+"char"(39)+"char"(10)
+'format bcp'+"char"(10) 
+'delimited by '+"char"(39)+"col_delimiter"+"char"(39)+"char"(10)
+'row delimited by '+"char"(39)+"row_delimiter"+"char"(39)+"char"(10)
+'on file error rollback'+"char"(10) 
+'notify 1000000'+"char"(10) 
+'quotes off'+"char"(10) 
+'escapes off;'+"char"(10) 
+'commit;' 
into "result"
from "sp_iqtable"()
where "Table_owner" = "owner"
and "Table_name" = "tbl";
select "result"
end 

call write_load('DBA','STG_cr__comast','/sybdata/mis/iapply_export', 'STG_cr__comast.bcp','|#|','!@#')

-- load proc
create or replace proc load_table(owner varchar(256),tbl varchar(256),iq_export_dir varchar(1024),filename varchar(1024),col_delimiter VARCHAR(4),row_delimiter varchar(4), out res varchar(20000))
begin
declare @dt datetime;
message '---------------------------------------------------' to client;
message '---- Load Table '+owner+'.'+tbl+' --------' to client;
select 'truncate table '+Table_owner+'.'+Table_name+';'+
'LOAD TABLE '+Table_owner+'.'+Table_name+'('+ col_list_bcp(Table_owner,Table_name)+')'+
'FROM '+ char(39) + iq_export_dir + '/'+ filename + char(39) + char(10) +
'format bcp' + char(10)+
'delimited by ' + char(39) + col_delimiter + char(39) + char(10) +
'row delimited by ' + char(39) + row_delimiter + char(39) + char(10) +
'on file error rollback'+char(10)+
'notify 1000000'+char(10)+
'quotes off'+char(10)+'escapes off;'+char(10)+
'commit;' into res
from sp_iqtable()
whereTable_owner=owner and 
Table_name =tbl;
set @dt=getdate();
execute immediateres;
message 'Load Duration for '+owner+'.'+tbl+'(sec): '+convert(varchar(10), datediff(ss,@dt,getdate())) to client;
message '---------------------------------------------------' to client;
end;

 
call load_table('DBA','STG_cr__comast','/sybdata/mis/iapply_export', 'STG_cr__comast.bcp','|#|','!@#')
