drop proc if exists "dba"."extract_data";

create procedure "dba"."extract_data"( in @tbl varchar(256),in @dir varchar(1024) )
begin
  declare @str1 varchar(1024);
  declare @str2 varchar(1024);
  declare @str3 varchar(1024);
  declare @str4 varchar(1024);
  declare @str5 varchar(1024);
  declare @str6 varchar(1024);
  select "col_list_for_export"("left"(@tbl,"charindex"('.',@tbl)-1),"right"(@tbl,"len"(@tbl)-"charindex"('.',@tbl)),'\x09') into #tmp;
  set temporary option "Temp_Extract_append" = 'OFF';
  set temporary option "Temp_Extract_Column_Delimiter" = '\x09';
  set temporary option "Timestamp_format" = 'YYYY-MM-DD HH:MM:SS';
  set temporary option "Temp_Extract_Quotes_All" = 'OFF';
  set @str1 = 'set temporary option Temp_Extract_Directory='+"char"(39)+@dir+"char"(39)+';';
  execute immediate @str1;
  set @str2 = 'set temporary option Temp_Extract_Name1='+"char"(39)+"substring"(@tbl,"charindex"('.',@tbl)+1,"len"(@tbl))+'.txt'+"char"(39)+';';
  execute immediate with result set on @str2;
  set @str3 = 'select * from #tmp;';
  execute immediate with result set on @str3;
  set temporary option "Temp_Extract_append" = 'ON';
  set temporary option "Temp_Extract_Column_Delimiter" = '\x09';
  set temporary option "Timestamp_format" = 'YYYY-MM-DD HH:MM:SS';
  set temporary option "Temp_Extract_Quotes_All" = 'ON';
  set temporary option "TEMP_EXTRACT_NULL_AS_EMPTY" = 'ON';
  set @str4 = 'set temporary option Temp_Extract_Directory='+"char"(39)+@dir+"char"(39)+';';
  execute immediate @str4;
  set @str5 = 'set temporary option Temp_Extract_Name1='+"char"(39)+"substring"(@tbl,"charindex"('.',@tbl)+1,"len"(@tbl))+'.txt'+"char"(39)+';';
  execute immediate with result set on @str5;
  set @str6 = 'select * from '+@tbl+';';
  execute immediate with result set on @str6
end;
