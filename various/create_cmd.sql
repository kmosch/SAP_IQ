/*
create a new IQ database and add the iq_main dbspace which is made the default dbspace
*/
create database '/sybdata/iq/kiq/kiq.db'
blank padding on
case respect
page size 8192
collation  '1253ELL'
dba user 'dba'
dba password 'sybase'
iq path '/sybdata/iq/kiq/kiq.iq'
iq size 500
iq page size 131072
temporary path '/sybdata/iq/kiq/kiq.iqtmp'
temporary size 500;


CREATE DBSPACE iq_main
USING FILE iq_main_01
'/sybdata/iq/kiq/iq_main_01' SIZE 512 MB IQ STORE;


GRANT CREATE ON iq_main TO PUBLIC;
REVOKE CREATE ON IQ_SYSTEM_MAIN FROM PUBLIC;
SET OPTION PUBLIC.DEFAULT_DBSPACE = 'iq_main';
