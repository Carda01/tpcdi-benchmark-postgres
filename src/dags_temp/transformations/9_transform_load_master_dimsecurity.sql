-- master.dimsecurity transform and load
truncate table master.dimsecurity;
with historical_finwire_sec as (
       select f.*,
       lead(pts) over (partition by symbol order by pts ASC) as next_pts
       from processing.finwire_sec f
)
insert into master.dimsecurity
	select 
		row_number() over() as sk_securityid,
		symbol,
		issuetype as issue,
		s.st_name as status,
		f.name,
		exid as exchangeid,
		c.sk_companyid as sk_companyid,
		shout::numeric(12) as sharesoutstanding,
		left(firsttradedate, 8)::date,
		left(firsttradeexchg, 8)::date,
		dividend::numeric(10,2),
		case
		    when f.next_pts is null
            then true
            else false
        end as iscurrent,
		1 as batchid,
		left(f.pts, 8)::date,
        case
		    when f.next_pts is null
            then '9999-12-31'::date
            else left(f.next_pts, 8)::date
        end as enddate
	from historical_finwire_sec f,
		master.statustype s,
		master.dimcompany c
	where f.status = s.st_id
	and ((ltrim(f.conameorcik, '0') = c.companyid::varchar) 
		or (f.conameorcik = c.name)
	    or (c.companyid = 0 and f.conameorcik = '0000000000'))
	and left(pts, 8)::date >= c.effectivedate
	and left(pts, 8)::date < c.enddate;