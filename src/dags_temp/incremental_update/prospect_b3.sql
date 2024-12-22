DROP TABLE inserted_row_counts;

with inserted_rows as(
insert into master.prospect
with prospect_prior as (
    SELECT
        agencyid,
        batchid,
				sk_updatedateid,
        lastname,
        firstname,
        middleinitial,
        gender,
        addressline1,
        addressline2,
        postalcode,
        city,
        state,
        country,
        phone,
        income,
        numbercars,
        numberchildren,
        maritalstatus,
        age,
        creditrating,
        ownorrentflag,
        employer,
        numbercreditcards,
        networth
    FROM master.prospect
    WHERE sk_updatedateid IS NOT NULL
),

prior_date as(
select prospect_prior.agencyid as agencyid, prospect_prior.sk_updatedateid as sk_updatedateid from
processing.prospect as p
inner join prospect_prior
on prospect_prior.agencyid = p.agencyid
and p.lastname = prospect_prior.lastname
AND p.firstname = prospect_prior.firstname
AND p.middleinitial = prospect_prior.middleinitial
AND p.gender = prospect_prior.gender
AND p.addressline1 = prospect_prior.addressline1
AND p.addressline2 = prospect_prior.addressline2
AND p.postalcode = prospect_prior.postalcode
AND p.city = prospect_prior.city
AND p.state = prospect_prior.state
AND p.country = prospect_prior.country
AND p.phone = prospect_prior.phone
AND p.income = prospect_prior.income
AND p.numbercars = prospect_prior.numbercars
AND p.numberchildren = prospect_prior.numberchildren
AND p.maritalstatus = prospect_prior.maritalstatus
AND p.age = prospect_prior.age
AND p.creditrating = prospect_prior.creditrating
AND p.ownorrentflag = prospect_prior.ownorrentflag
AND p.employer = prospect_prior.employer
AND p.numbercreditcards = prospect_prior.numbercreditcards
AND p.networth = prospect_prior.networth
),

batchdate as (
  select sk_dateid
  from master.dimdate
  where datevalue = (select batchdate from processing.batchdate)
)

	select
	  p.agencyid
	, (select sk_dateid from batchdate)
	, case
	  when (select count(*) from prior_date where p.agencyid= prior_date.agencyid)>0
		then (select distinct sk_updatedateid from prior_date where p.agencyid= prior_date.agencyid)
		else (select sk_dateid from batchdate)
		end
	, 3 as batchid
	, false
	, p.lastname
	, p.firstname
	, p.middleinitial
	, p.gender
	, p.addressline1
	, p.addressline2
	, p.postalcode
	, p.city
	, p.state
	, p.country
	, p.phone
	, p.income
	, p.numbercars
	, p.numberchildren
	, p.maritalstatus
	, p.age
	, p.creditrating
	, p.ownorrentflag
	, p.employer
	, p.numbercreditcards
	, p.networth
	, NULLIF(CONCAT_WS('+',
        CASE
            WHEN p.networth > 1000000 OR p.income > 200000 THEN 'HighValue'
            ELSE NULL
        END,
        CASE
            WHEN p.numberchildren > 3 OR p.numbercreditcards > 5 THEN 'Expenses'
            ELSE NULL
        END,
        CASE
            WHEN p.age > 45 THEN 'Boomer'
            ELSE NULL
        END,
        CASE
            WHEN p.income < 50000 OR p.creditrating < 600 OR p.networth < 100000 THEN 'MoneyAlert'
            ELSE NULL
        END,
        CASE
            WHEN p.numbercars > 3 OR p.numbercreditcards > 7 THEN 'Spender'
            ELSE NULL
        END,
        CASE
            WHEN p.age < 25 AND p.networth > 1000000 THEN 'Inherited'
            ELSE NULL
        END
    ), '')
	from processing.prospect p
RETURNING *)

select count(*) into inserted_row_counts from inserted_rows;
