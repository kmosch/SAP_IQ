create FUNCTION "dba"."col_list_binary" (@table_owner varchar(100),@table_name varchar(100))
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
select @col_num = count(*) fromSYSCOLUMN where table_id= @table_id
while @icount <= @col_num
begin
    select @temp_col = column_name from SYSCOLUMN where (table_id = @table_id) and (column_id = @icount)
    if @icount = @col_num
    begin
        select @column_list = @column_list + '['+ @temp_col + ']' + ' BINARY WITH NULL BYTE'+ char(10)
    end
    else
    begin
        select @column_list = @column_list + '['+ @temp_col + ']'+ ' BINARY WITH NULL BYTE,'+ char(10)
    end
    select @icount = @icount + 1
end
return @column_list;

drop proc if exists "dba"."col_list_text_export";
create or replace FUNCTION "DBA"."col_list_text_export" (@table_owner varchar(100),@table_name varchar(100), @delimiter varchar(4))
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