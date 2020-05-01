v.1 Procs for export and loading of data
Each script contains the needed IQ procs for exporting and loadind back this export.
They can be used independently for either import or exporting data.
Each proc 
1) counts the number of rows
2) measures the duration
3) sends log to the client

There are 3 sets of procedures
1) export/import in binary format (extract_load_binary.sql)
  a) procedure dba.extract_data_binary to export a table in file in binary format
  b) procedure dba.unset_options to unset the options set by dba.extract_data_binary
  c) function dba.col_list_binary to return the column list for the load command
  d) procedure dba.load_table_binary which loads a table from a file
  e) procedure dba.write_load_binary which returns the load command executed by dba.load_table_binary, used for debugging
2) export/import as text (extract_data.sql)
  a) procedure dba.extract_data to export a table in file as text
  b) procedure dba.unset_options to unset the options set by dba.extract_data
  c) function dba.col_list to return the column list for the load command
  d) procedure dba.load_table which loads a table from a file
  e) procedure dba.write_load which returns the load command executed by dba.load_table, used for debugging
3) import bcp output (load_bcp.sql)
  a) function dba.col_list_bcp to return the column list for the load command
  b) procedure dba.load_table_bcp which loads a table from a file
  c) procedure dba.write_load_bcp which returns the load command executed by dba.load_table_bcp, used for debugging