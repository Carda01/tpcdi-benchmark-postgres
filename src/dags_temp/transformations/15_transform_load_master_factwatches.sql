-- factwatches
truncate table master.factwatches;
insert into master.factwatches
	with watches as (
		select w1.w_c_id, 
		TRIM(w1.w_s_symb) as w_s_symb, 
		w1.w_dts::date as dateplaced, 
		w2.w_dts::date as dateremoved
		from processing.watchhistory w1,
			processing.watchhistory w2
		where w1.w_c_id = w2.w_c_id
		and w1.w_s_symb = w2.w_s_symb
		and w1.w_action = 'ACTV'
		and w2.w_action = 'CNCL'
	) 

	select
		c.sk_customerid as sk_customerid,
		s.sk_securityid as sk_securityid,
		to_char(w.dateplaced, 'yyyymmdd')::numeric as sk_dateid_dateplaced,
		to_char(w.dateremoved, 'yyyymmdd')::numeric as sk_dateid_dateremoved,
		1 as batchid
	from watches w
	join master.dimcustomer c
		on w.w_c_id = c.customerid
		and c.effectivedate <= w.dateplaced
		and w.dateplaced <= c.enddate
	join master.dimsecurity s
		on w.w_s_symb = s.symbol
	join master.dimdate d1
		on w.dateplaced = d1.datevalue
	join master.dimdate d2
		on w.dateremoved = d2.datevalue;
