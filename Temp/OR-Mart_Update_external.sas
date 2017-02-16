/********************************************************************************************************/
/*  FLEXMART EXTERNAL DATA                                                                              */
/*	0.7 2014-12-08 FEGO01 Introduced _FIRSTOBS/_TERMSTR                                              */
/*	0.8 2014-12-09 FEGO01 VALID_FROM/_TO BASE_DT                                                     */
/*	0.9 2015-01-26 FEGO01 BASE_DT span                                                               */
/*	1.0 2015-02-17 FEGO01 POPS                                                                       */
/*	1.1 2015-02-27 FEGO01 Replaced _TERMSTR col with macro                                           */
/*	1.2 2015-03-12 FEGO01 TERMSTR macro change and separate Ultimo                                   */
/*	1.2 2015-03-12 FEGO01 Added X_EXTERNAL_FILE_PATH                                                 */
/*	1.3 2015-04-02 GUBE15 Added function for external XRLU                                           */
/*	1.4 2015-11-18 GUBE15 Added RUN_ON_DAILY/ULTIMO for External/XRLU Data                           */
/*	1.5 2016-05-23 FEGO01 VALID_FROM_BASEDATE <= "&_base_dt."d <= VALID_TO_BASEDATE                  */
/*	1.6 2016-10-12 OSHA01 Added input parameter _DELETE_MD (DT/SK) for deleting old data             */
/*     1.7 2017-01-17 PALI05 Changed loading into partitioned ext table                                                                                                 */
/*                                                                                                      */
/********************************************************************************************************/
 
options LOCALE=EN_US;
%global _select_sql;
%let _select_sql=;
%global _insert_sql;
%let _insert_sql=;

%macro termstr(file);

  %local savopts;
  %global _termstr_;
  %let _termstr_=LF;
  %let savopts=%sysfunc(getoption(NOTES));

  options nonotes;

  data _null_;
    infile "&file" pad lrecl=1028 recfm=F;
    input;
    *- Look for the carriage-return line-feed double character and if   -;
    *- we find it then assume that CRLF is the line termination string. -;
    if index(_infile_,"0D0A"X) then call symput('_termstr_','CRLF');
	/*if index(_infile_,cats('0D'x)) then call symput('_termstr_','CRLF');
	call symput('_substr_',substr(_infile_,1,2000));*/
    stop;
  run;

  %put %trim(NO)TE: Delimiter is: &_termstr_.;

  options &savopts;

%mend termstr;


%macro removeRowsWithMissingVals(inDsn, outDsn, Exclusion);
/*Inputs: 
        inDsn: Input dataset with some or all columns missing for some or all rows
        outDsn: Output dataset with some or all columns NOT missing for some or all rows
        Exclusion: Should be one of {AND, OR}. AND will only exclude rows if any columns have missing values, OR will exclude only rows where all columns have  missing values
*/
/*get a list of variables in the input dataset along with their types (i.e., whether they are numericor character type)*/
PROC CONTENTS DATA = &inDsn OUT = CONTENTS(keep = name type varnum);
RUN;
/*put each variable with its own comparison string in a seperate macro variable*/
data _null_;
set CONTENTS nobs = num_of_vars end = lastObs;
/*use NE. for numeric cols (type=1) and NE '' for char types*/
if type = 1 then            call symputx(compress("var"!!varnum), compbl(name!!" NE . "));
else        call symputx(compress("var"!!varnum), compbl(name!!" NE ''  "));
/*make a note of no. of variables to check in the dataset*/
if lastObs then call symputx("no_of_obs", _n_);
run;

DATA &outDsn;
set &inDsn;
where
%do i =1 %to &no_of_obs.;
    &&var&i.
        %if &i < &no_of_obs. %then &Exclusion; 
%end;
;
run;

%mend removeRowsWithMissingVals;


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
 
%let _select_sql=&_cols_concat.;
%let _insert_sql=&_master_cols_concat.;
%mend;


/* Behandla nya filer, lägg de i den gemensamma infilstabellen */
	

%macro PROCESS_INFILES(_CONTROL, _PATH, _PROJECT_SK, _DELETE_MD);


	
	/* Vilken basedate gäller det? */
	proc sql;
	select BASE_DT, upcase(DELIVERY_TYPE) into :_base_dt, :_delivery_type
	from ORMART.X_PROJECT_DIM
	WHERE PROJECT_SK=&_project_sk.
	;
	quit;
	%put %trim(NO)TE: BASE_DT is &_base_dt.;

	/* Skapa returmeddelande för användarna */
	%global execution_user_message;
	%let execution_user_message=External data imported for &_base_dt.;

	proc sql;
	select distinct 
	cats(TARGET_LIBNAME,".",TARGET_TABLE) as TARGET, TARGET_LIBNAME into : _target separated by ',', : _target_libname separated by ','
	from &_CONTROL.
	where VALID_FROM_BASEDATE <= "&_base_dt."d <= VALID_TO_BASEDATE;
	quit;


	%DO w=1 %TO %sysfunc(countw("&_target",","));

              /*Kolla om måltabellen skall parallelliseras */
              %IF &_TARGET.=ORMART.X_CLASSIFICATION_DIM_EXT %THEN %DO;
              %LET _TARGET=ORMART.X_CLASSIFICATION_DIM_EXT_&_PROJECT_SK.;
              %END;

              %IF &_TARGET.=ORMART.X_CASHFLOW_FACT_EXT %THEN %DO;
              %LET _TARGET=ORMART.X_CASHFLOW_FACT_EXT_&_PROJECT_SK.;
              %END;


		%IF &_DELETE_MD. = "SK" %THEN %DO;
		/* Rensa eventuella existerande rader för aktuell PROJECT_SK */
			%IF %UPCASE(&_target_libname) = ORMART %THEN %DO;	
				proc sql;
				connect using ORMART; 
				execute (
				delete from %SCAN(%SUPERQ(_target),&w.,',')
				where PROJECT_SK=&_project_sk. 
				;
				) by db2;
				disconnect from db2 ;
				;
				quit;
			%END;
			%ELSE %DO;		
				proc sql;
				delete from %SCAN(%SUPERQ(_target),&w.,',')
				where PROJECT_SK=&_project_sk. 
				;
				quit;
			%END;
		%END;
		%ELSE %IF &_DELETE_MD. = "DT" %THEN %DO;
		/* Rensa eventuella existerande rader för aktuell BASE_DT */
			%IF %UPCASE(&_target_libname) = ORMART %THEN %DO;	
				proc sql;
				connect using ORMART; 
				execute (
				delete from %SCAN(%SUPERQ(_target),&w.,',')
				where BASE_DT="&_base_dt."D 
                            and project_sk not in(select distinct project_sk from ormart.x_project_dim where entity_id='CETR')
				;
				) by db2;
				disconnect from db2 ;
				;
				quit;
			%END;
			%ELSE %DO;		
				proc sql;
				delete from %SCAN(%SUPERQ(_target),&w.,',')
				where BASE_DT="&_base_dt."D 
                            and project_sk not in(select distinct project_sk from ormart.x_project_dim where entity_id='CETR')
				;
				quit;
			%END;
		%END;

	%END;

	%put PATH is &_PATH.;
	/* Uppdatera tillgängliga akriv */
	filename archives pipe "find &_PATH./*.txt -maxdepth 1 -not -type d -printf '%NRSTR(%f\n)'";
	/* */;
	filename archfull pipe "find &_PATH./*.txt -maxdepth 1 -not -type d";
	/* */;

	data FULL;
		length FULL_NAME $ 256;
	   infile archfull encoding="utf-8";
	   input FULL_NAME $;
	   NR=_N_;
	run;

	data SHORT;
		length FILE_NAME $ 256
		SOURCE_SYSTEM $ 256
		SOURCE_ID $ 256
		ORIGINAL_BASEDATE 8
		DELIVERY_DTTM 8
		;
		format ORIGINAL_BASEDATE date9.
		DELIVERY_DTTM datetime16.
		;
	   	infile archives encoding="utf-8";
	   	input FILE_NAME $;
	   	NR=_N_;
		SOURCE_SYSTEM=scan(FILE_NAME,1,'_');
		SOURCE_ID=scan(FILE_NAME,2,'_');
		ORIGINAL_BASEDATE=input(scan(FILE_NAME,3,'_'), yymmdd.);
		DELIVERY_TIME_STR=scan(scan(FILE_NAME,5,'_'),1,'.');
		DELIVERY_TIME_STR=substr(DELIVERY_TIME_STR,1,length(DELIVERY_TIME_STR)-2)||':'||substr(DELIVERY_TIME_STR,length(DELIVERY_TIME_STR)-1,2);
		DELIVERY_DTTM_STR=scan(FILE_NAME,4,'_')||' '||DELIVERY_TIME_STR;
		DELIVERY_DTTM=input(DELIVERY_DTTM_STR, anydtdtm.);
		IF ORIGINAL_BASEDATE =< input("&_base_dt", date9.) THEN Output;
	run;


	proc sql;
		create table INFILE_LIST as
		select f.FULL_NAME as FULL_PATH, s.* from FULL f
		inner join SHORT s
		on f.NR=s.NR
		;
	quit;

	/* Se till att endast senaste BASE_DT läses in */
	proc sql;
	create table _only_latest_basedate as
	select * from INFILE_LIST
	group by SOURCE_SYSTEM||SOURCE_ID
	having ORIGINAL_BASEDATE=max(ORIGINAL_BASEDATE)
	;
	quit;



	/* Se till att endast senaste filversion läses in */
	proc sql;
	create table _only_latest_files as
	select * from _only_latest_basedate
	group by SOURCE_SYSTEM||SOURCE_ID||put(ORIGINAL_BASEDATE, date9.)
	having DELIVERY_DTTM=max(DELIVERY_DTTM)
	;
	quit;


	/* Läs in filerna en och en till infilstabeller */
	
	%let _nr_files=&SQLOBS.;
	%put %trim(NO)TE: Number of files to process: &_nr_files.;
	
	/* Skapa macrovektor med alla filer att läsa in */
		data _infiles;
			set _only_latest_files;

			call symput('_file_'||strip(put(_N_, best12.)), FULL_PATH);
			call symput('_file_name_'||strip(put(_N_, best12.)), FILE_NAME);
		run;
	


	%let j=1;

	%DO j=1 %TO &_nr_files.;



		%put %trim(NO)TE: Processing file %CMPRES(&&_file_&j.) into table file_%cmpres(&j.).;
		%put DATA file_%cmpres(&j.).;
		%put _FILE_NAME_&j.=&&_FILE_NAME_&j.;
		%put SOURCE_SYSTEM is %trim(%qscan(%CMPRES(&&_FILE_NAME_&j.),1,'_'));
		%let _source_system=%trim(%qscan(%CMPRES(&&_FILE_NAME_&j.),1,'_'));
		%put SOURCE_ID is %trim(%qscan(%CMPRES(&&_FILE_NAME_&j.),2,'_'));
		%let _source_id=%trim(%qscan(%CMPRES(&&_FILE_NAME_&j.),2,'_'));
		%put ORIGINAL_BASEDATE is %trim(%qscan(%CMPRES(&&_FILE_NAME_&j.),3,'_'));
		%let _original_basedate=%trim(%qscan(%CMPRES(&&_FILE_NAME_&j.),3,'_'));


		/* Skapa vektor med kolumner */
		proc sql;
		create table _sub_config as
		select *
		from &_CONTROL.
		where 	SOURCE_SYSTEM=scan("&&_FILE_NAME_&j.",1,'_')
		and SOURCE_ID=scan("&&_FILE_NAME_&j.",2,'_')
/*		and strip(FILE_PATH)=strip("_PATH")*/
		and VALID_FROM_BASEDATE <= input("&_original_basedate.",yymmdd8.) <= VALID_TO_BASEDATE
/* Filter on delivery BASE_DT */
		and VALID_FROM_BASEDATE <= "&_base_dt."d <= VALID_TO_BASEDATE
		and (("&_delivery_type." = "ULTIMO" AND RUN_ON_ULTIMO = 1) OR ("&_delivery_type." = "DAILY" AND RUN_ON_DAILY = 1))
		;
		quit;
		
		%let _no_cols=&SQLOBS.;
		%IF &SQLOBS. > 0 %THEN %put %trim(NO)TE: Import configuration found for file &&_file_&j.;

		
		proc sql;
		select SKIP_BlANK_ROWS into :_skip_blank_rows
		from _SUB_CONFIG(obs=1)
		;
		quit;

		proc sql;
		select DELIMITER into :_delimiter
		from _SUB_CONFIG(obs=1)
		;
		quit;

		proc sql;
		select FIRSTOBS into :_firstobs
		from _SUB_CONFIG(obs=1)
		;
		quit;

		proc sql;
		select TARGET_LIBNAME into :_target_libname
		from _SUB_CONFIG(obs=1)
		;
		quit;

		proc sql;
		select TARGET_TABLE into :_target_table
		from _SUB_CONFIG(obs=1)
		;
		quit;

		%let _filter=;
		proc sql;
		select FILTER into :_filter
		from _SUB_CONFIG(obs=1)
		;
		quit;
		%put %trim(NO)TE: FILTER=&_filter.;

		%IF %LENGTH(%cmpres(_filter))>2 %THEN %let _filter=%cmpres(%BQUOTE(&_filter.));
		
		
		proc sql;
		select 
		SOURCE_COLUMN, TARGET_COLUMN, INFORMAT, FORMAT, SKIP_BLANK_ROWS, FILTER into :_source_col1-:_source_col1000, :_target_col1-:_target_col1000, 
		:_informat_col1-:_informat_col1000, :_format_col1-:_format_col1000, :_skip_blank_col1-:_skip_blank_col1000, :_filter_col1-:_filter_col1000
		from _SUB_CONFIG
		;
		quit;



		/* Skapa DATA-steg för import */

		%IF &SQLOBS. > 0 %THEN %DO;
		/* Kontrollera filens TERMSTR */
		%termstr(%CMPRES(&&_file_&j.));

		DATA file_%cmpres(&j.);

		INFILE "%CMPRES(&&_file_&j.)" DELIMITER = &_delimiter. MISSOVER DSD LRECL=32767 FIRSTOBS=&_firstobs. TERMSTR=&_termstr_.;

		/*	Skapa INFORMAT-argument	*/
		%DO i=1 %TO &_no_cols.;
			%put INFORMAT &&_target_col&i. &&_informat_col&i...;
			INFORMAT &&_target_col&i. &&_informat_col&i...;
		%END;
		
		/* Skapa FORMAT-argument */
		%DO f=1 %TO &_no_cols.;
			%put FORMAT &&_target_col&f. &&_format_col&f...;
			FORMAT &&_target_col&f. &&_format_col&f...;
		%END;
		FORMAT ORIGINAL_BASEDATE date9.;		
		FORMAT BASE_DT date9.;


		/* Skapa INPUT-argument	*/
		 
		%let _input=;
		%DO i=1 %TO &_no_cols.;
			%let _input=&_input. &&_target_col&i.;
			%IF %SUBSTR(&&_format_col&i.., 1, 1)=$ %THEN %let _input=&_input. $;
		%END;
		%put INPUT &_input.;
		INPUT &_input.;
		
		SOURCE_SYSTEM="&_source_system.";
		SOURCE_ID="&_source_id.";
		ORIGINAL_BASEDATE=input("&_original_basedate.", yymmdd.);
		BASE_DT=input("&_base_dt.", date9.);
		X_EXTERNAL_FILE_PATH="%CMPRES(&&_file_&j.)";
              X_ROW_NR = _N_; 

		%put %trim(NO)TE: Filter is %cmpres(%BQUOTE(&_filter.));
		%IF %length(%CMPRES(&_filter.))>3 %THEN %DO;
		IF %cmpres(%BQUOTE(&_filter.)) THEN OUTPUT;
		%END;
		
		
		RUN;


		*put SKIP BLANK: &_skip_blank_rows.;
		
		%IF &_skip_blank_rows. > 0 %THEN %DO;
		%put %trim(NO)TE: Skip blank enabled - purging blank rows;
		%removeRowsWithMissingVals(file_%cmpres(&j.), file_%cmpres(&j.), AND);
		%END;


		data file_%cmpres(&j.);
			set file_%cmpres(&j.);
			PROJECT_SK=&_PROJECT_SK.;
                    
		run;

		/* Exportera inlästa filer till måltabell för inläsningar */
		%CREATE_MERGED_SQL(&_TARGET_LIBNAME..&_TARGET_TABLE., work.file_%cmpres(&j.), ., '');
		

		/* Spara undan inläst data! */

              /*Kolla om måltabellen skall parallelliseras */
              %IF &_TARGET_TABLE.=X_CLASSIFICATION_DIM_EXT %THEN %DO;
              %LET _TARGET_TABLE=X_CLASSIFICATION_DIM_EXT_&_PROJECT_SK.;
              %END;

              %IF &_TARGET_TABLE.=X_CASHFLOW_FACT_EXT %THEN %DO;
              %LET _TARGET_TABLE=X_CASHFLOW_FACT_EXT_&_PROJECT_SK.;
              %END;



		
		/* Importera inlästa rader till måltabell */

            

		proc sql;
		insert into &_TARGET_LIBNAME..&_TARGET_TABLE.(&_insert_sql.)
		select &_select_sql. from work.file_%cmpres(&j.)
		;
		quit;
		%END;

	%END;


%mend PROCESS_INFILES;
/*%PROCESS_INFILES(work.ORM_EXTERNAL_DATA_CONFIG, /sasexcl/Lev1/CERM/CERMR_MART/Custom/Input/External, work.X_ClASSIFICATION_DIM_EXT, 961);*/
/*%PROCESS_INFILES(MARTCTRL.ORM_EXTERNAL_DATA_CONFIG, /sasexcl/Lev1/ORM/OR_MART/Custom/Input/External, ORMART.X_ClASSIFICATION_DIM_EXT, 1035);*/

%macro PROCESS_SPAN(_CONTROL, _PATH, _PROJECT_SK_FROM, _PROJECT_SK_TO, _DELETE_MD);

%put %trim(NO)TE: PROJECT_SK_FROM is &_project_sk_from.;
%put %trim(NO)TE: PROJECT_SK_TO is &_project_sk_to.;	

%put &_PROJECT_SK_FROM., &_PROJECT_SK_TO.;

	proc sql;
/*	create table _1 as*/
	select BASE_DT into :base_dt_from
	from martview.v_x_project_dim
	where PROJECT_SK=&_PROJECT_SK_FROM.
	;
	quit;

	proc sql;
/*	create table _2 as*/
	select BASE_DT into :base_dt_to
	from martview.v_x_project_dim
	where PROJECT_SK=&_PROJECT_SK_TO.
	;
	quit;


	proc sql;
	select distinct BASE_DT into :base_dt_1-:base_dt_999
	from martview.v_x_project_dim
	where BASE_DT >= "&BASE_DT_FROM."d and BASE_DT <= "&base_dt_to."d and ENTITY_ID="ORKF"
	order by BASE_DT
	;
	quit;

	%let _no_base_dt=&SQLOBS;

	proc sql;
	select distinct PROJECT_SK into :proj_sk_1-:proj_sk_9999
	from martview.v_x_project_dim
	where BASE_DT >= "&BASE_DT_FROM."d and BASE_DT <= "&base_dt_to."d
       and ENTITY_ID="ORKF"
	;
	quit;

	%let _no_proj_sk=&SQLOBS;

	%DO q=1 %TO &_no_proj_sk.;
		proc sql;
		select BASE_DT into :_base_dt
		from martview.v_x_project_dim
		where PROJECT_SK=&&proj_sk_&q.
		;
		quit;
		
		%put %trim(NO)TE: Processing PROJECT_SK &&proj_sk_&q. with BASE_DT &_base_dt.;
	
		%PROCESS_INFILES(&_CONTROL., &_PATH., &&proj_sk_&q., &_DELETE_MD);
	
	%END;

	/* Skapa returmeddelande för användarna */
	%global execution_user_message;
	%let execution_user_message=External data imported for &BASE_DT_FROM.-&BASE_DT_TO.;
	%put &execution_user_message.;
	

%mend PROCESS_SPAN;

/*%PROCESS_SPAN(MARTCTRL.ORM_EXTERNAL_DATA_CONFIG, /sasexcl/Lev1/ORM/OR_MART/Custom/Input/External, ORMART.X_ClASSIFICATION_DIM_EXT, &project_sk_from., &project_sk_to.);


%PROCESS_INFILES(MARTCTRL.ORM_EXTERNAL_XRLU_CONFIG, /sasexcl/Lev1/ORM/OR_MART/Custom/Input/TEST2, 1045);
%PROCESS_INFILES(MARTCTRL.ORM_EXTERNAL_DATA_CONFIG, /sasexcl/Lev1/ORM/OR_MART/Custom/Input/External, 1045);

%PROCESS_SPAN(MARTCTRL.ORM_EXTERNAL_XRLU_CONFIG, /sasexcl/Lev1/ORM/OR_MART/Custom/Input/TEST2, 1035, 1035);*/




