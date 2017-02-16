%macro CREATE_MERGED_SQL(master_table, slave_table, missing_num, missing_str);
%put MASTER TABLE is &master_table.;
%put SLAVE TABLE is &slave_table.;
%put MISSING NUM is &missing_num.;
%put MISSING STR is &missing_str.;



/* Skapa lista över tillgängliga master-kolumner */
proc sql;
create table _master_cols as
select name, type from dictionary.columns
where memtype = "DATA"
	and upcase(strip(libname)) = UPCASE(scan(strip("&master_table"), 1))
	and upcase(strip(memname)) = UPCASE(scan(strip("&master_table"), 2))
order by name
;
quit;



/* Lista master-kolumner som saknas i slav-tabellen och skall ersättas med logisk kolumn */

proc sql;
create table _missing_master_cols as
select * from _master_cols
where name not in
(
select name from dictionary.columns
where memtype = "DATA" 
	and upcase(strip(libname)) = UPCASE(scan(strip("&slave_table"), 1))
	and upcase(strip(memname)) = UPCASE(scan(strip("&slave_table"), 2))
)
order by name
;
quit;


proc sql;
create table _master_cols_flags as
select a.name, a.type, (case
when a.name=b.name then 1 else 0
end) as missing_flg
from _master_cols a left join _missing_master_cols b
		on a.name=b.name
;
quit;





data _null_;
set _master_cols_flags end=eof;
length master_cols_concat $ 10000;
retain master_cols_concat;

*put name;
*put missing_flg;

call symput("_master_col"||strip(put(_N_, best12.)), strip(name));

if missing_flg=0 then
call symput("_col"||strip(put(_N_, best12.)), strip(name));

if missing_flg=1 and type='num' then
call symput("_col"||strip(put(_N_, best12.)), "&missing_num. as "||strip(name));

if missing_flg=1 and type='char' then
call symput("_col"||strip(put(_N_, best12.)), "&missing_str. as "||strip(name));

if eof then call symput('_nr_cols',_N_);
run;

%let i=1;
%let _cols_concat=&&_col&i. ;
%let _master_cols_concat=&&_master_col&i.;

%DO i=2 %TO &_nr_cols.;
	%let _cols_concat=&_cols_concat., &&_col&i.;
	%let _master_cols_concat=&_master_cols_concat., &&_master_col&i.;
%END;

/*%put MASTER_COLS_CONCAT: &_master_cols_concat.;
*%put COLS_CONCAT: &_cols_concat.;*/
 
%global _select_sql;
%global _insert_sql;

%let _select_sql=&_cols_concat.;
%put %trim(NO)TE: SELECT is &_select_sql.;
%let _insert_sql=&_master_cols_concat.;
%put %trim(NO)TE: INSERT is &_insert_sql.;

%mend;

%CREATE_MERGED_SQL(ORMART.X_CLASSIFICATION_DIM, ORMART.X_CLASSIFICATION_DIM_EXT, null, '');

proc sql;
connect using ORMART; 
execute (drop view ORMART.V_X_CLASS_DIM) by db2 ;
disconnect from db2 ;
;
quit;	

proc sql;
connect using ORMART; 
execute (create or replace view ORMART.V_X_CLASS_DIM as 
select &_select_sql., 
SOURCE_SYSTEM, SOURCE_ID, ORIGINAL_BASEDATE, EXT_COL_1, EXT_COL_2, EXT_COL_3,
EXT_COL_4, EXT_COL_5, EXT_COL_6, EXT_COL_7, EXT_COL_8, EXT_COL_9, EXT_COL_10, EXT_COL_11, EXT_COL_12, 
EXT_COL_13, EXT_COL_14, EXT_COL_15, EXT_COL_16, EXT_COL_17, EXT_COL_18, EXT_COL_19, EXT_COL_20
, EXT_COL_21, EXT_COL_22, EXT_COL_23, EXT_COL_24, EXT_COL_25, EXT_COL_26, EXT_COL_27, EXT_COL_28, EXT_COL_29, EXT_COL_30
, EXT_COL_31, EXT_COL_32, EXT_COL_33, EXT_COL_34, EXT_COL_35, EXT_COL_36, EXT_COL_37, EXT_COL_38, EXT_COL_39, EXT_COL_40
, X_EXTERNAL_FILE_PATH
from ORMART.X_CLASSIFICATION_DIM_EXT_ORKF

union all

select &_insert_sql.,
'RICI' as SOURCE_SYSTEM, '' as SOURCE_ID, null as ORIGINAL_BASEDATE, '' as EXT_COL_1, '' as EXT_COL_2, '' as EXT_COL_3, 
'' as EXT_COL_4, '' as EXT_COL_5, '' as EXT_COL_6, '' as EXT_COL_7, '' as EXT_COL_8, '' as EXT_COL_9, '' as EXT_COL_10, '' as 
EXT_COL_11, '' as EXT_COL_12, '' as EXT_COL_13, '' as EXT_COL_14, '' as EXT_COL_15, '' as EXT_COL_16, '' as EXT_COL_17, '' as 
EXT_COL_18, '' as EXT_COL_19, '' as EXT_COL_20, '' as EXT_COL_21, '' as EXT_COL_22, '' as EXT_COL_23, '' as EXT_COL_24, '' as 
EXT_COL_25, '' as EXT_COL_26, '' as EXT_COL_27, '' as EXT_COL_28, '' as EXT_COL_29, '' as EXT_COL_30, '' as EXT_COL_31, '' as 
EXT_COL_32, '' as EXT_COL_33, '' as EXT_COL_34, '' as EXT_COL_35, '' as EXT_COL_36, '' as EXT_COL_37, '' as EXT_COL_38, '' as 
EXT_COL_39, '' as EXT_COL_40, '' as X_EXTERNAL_FILE_PATH
from ORMART.X_CLASSIFICATION_DIM_ORKF) by db2 ;
disconnect from db2 ;
;
quit;
 
proc sql;
connect using ORMART; 
execute (drop view ORMART.V_X_CASH_FACT) by db2 ;
disconnect from db2 ;
;
quit;	

proc sql;
connect using ORMART; 
execute (create or replace view ORMART.V_X_CASH_FACT as 
select *
from ORMART.X_CASHFLOW_FACT_ORKF
union all 
select *
from ORMART.X_CASHFLOW_FACT_EXT_ORKF
) by db2 ;
disconnect from db2 ;
;
quit;

%put &sysuserid.;

%macro GRANT_CONTROL_ORMART;
%put %trim(NO)TE: User is &sysuserid.;
%IF %UPCASE(&sysuserid.)=%UPCASE(ricipr1g) %THEN %DO;
proc sql;
connect using ORMART; 
execute (grant select, update, insert, delete, control on ORMART.V_X_CLASS_DIM to user RICIPR3G) by db2 ;
disconnect from db2 ;
;
quit;

proc sql;
connect using ORMART; 
execute (grant select, update, insert, delete, control on ORMART.V_X_CASH_FACT to user RICIPR3G) by db2 ;
disconnect from db2 ;
;
quit;
%END;
%mend;
%GRANT_CONTROL_ORMART;
