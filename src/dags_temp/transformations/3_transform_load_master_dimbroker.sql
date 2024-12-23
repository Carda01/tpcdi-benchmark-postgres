-- dimbroker
truncate table master.dimbroker;
with min_date_cte as (
  select min(datevalue) as mindate
  from master.dimdate
)
insert into master.dimbroker
	select 
	row_number() over(order by employeeid) as sk,
	employeeid as brokerid,
	managerid,
	employeefirstname,
	employeelastname,
	employeemi,
	employeebranch,
	employeeoffice,
	employeephone,
	true as iscurrent,
	1 as batchid,
	(select mindate FROM min_date_cte) as effectivedate,
	'9999-12-31'::date as enddate
	from processing.hr
	where employeejobcode = 314;
