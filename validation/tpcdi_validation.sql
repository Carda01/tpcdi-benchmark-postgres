/* ++++++++++++++++++++++++++++++++++++++++++++++++++ *
 * +                                                + *
 * +        TPC-DI  Validation Query                + *
 * +        Version 1.1.0                           + *
 * +                                                + *
 * ++++++++++++++++++++++++++++++++++++++++++++++++++ *
 *                                                    *
 *       ====== Portability Substitutions ======      *
 *     ---        [FROM DUMMY_TABLE]    ------        *
 * DB2            from sysibm.sysdummy1               *
 * ORACLE         from dual                           *
 * SQLSERVER       <blank>                            *
 * -------------------------------------------------- *
 *     ------  [||] (String concatenation) ------     *    
 * SQLSERVER      +                                   *
 * -------------------------------------------------- *
 */

insert into master.Dimessages

select

     CURRENT_TIMESTAMP as MessageDateAndTime
     ,case when BatchID is null then 0 else BatchID end as BatchID
     ,MessageSource
     ,MessageText 
     ,'Validation' as MessageType
     ,MessageData

from (
     select max(BatchID) as BatchID from master.Dimessages 
) x join (

    /* Basic row counts */
     select 'master.DimAccount' as MessageSource, 'Row count' as MessageText, count(*) as MessageData from master.DimAccount
     union select 'master.DimBroker' as MessageSource, 'Row count' as MessageText, count(*) as MessageData from master.DimBroker
     union select 'master.DimCompany' as MessageSource, 'Row count' as MessageText, count(*) as MessageData from master.DimCompany
     union select 'master.DimCustomer' as MessageSource, 'Row count' as MessageText, count(*) as MessageData from master.DimCustomer
     union select 'master.DimDate' as MessageSource, 'Row count' as MessageText, count(*) as MessageData from master.DimDate
     union select 'master.DimSecurity' as MessageSource, 'Row count' as MessageText, count(*) as MessageData from master.DimSecurity
     union select 'master.DimTime' as MessageSource, 'Row count' as MessageText, count(*) as MessageData from master.DimTime
     union select 'master.DimTrade' as MessageSource, 'Row count' as MessageText, count(*) as MessageData from master.DimTrade
     union select 'master.FactCashBalances' as MessageSource, 'Row count' as MessageText, count(*) as MessageData from master.FactCashBalances
     union select 'master.FactHoldings' as MessageSource, 'Row count' as MessageText, count(*) as MessageData from master.FactHoldings
     union select 'master.FactMarketHistory' as MessageSource, 'Row count' as MessageText, count(*) as MessageData from master.FactMarketHistory
     union select 'master.FactWatches' as MessageSource, 'Row count' as MessageText, count(*) as MessageData from master.FactWatches
     union select 'master.Financial' as MessageSource, 'Row count' as MessageText, count(*) as MessageData from master.Financial
     union select 'master.Industry' as MessageSource, 'Row count' as MessageText, count(*) as MessageData from master.Industry
     union select 'master.Prospect' as MessageSource, 'Row count' as MessageText, count(*) as MessageData from master.Prospect
     union select 'master.StatusType' as MessageSource, 'Row count' as MessageText, count(*) as MessageData from master.StatusType
     union select 'master.TaxRate' as MessageSource, 'Row count' as MessageText, count(*) as MessageData from master.TaxRate
     union select 'master.TradeType' as MessageSource, 'Row count' as MessageText, count(*) as MessageData from master.TradeType
     /* Joined row counts for Fact tables */
     union select 'master.FactCashBalances' as MessageSource, 'Row count joined' as MessageText, 
			count(*) as MessageData 
			from master.FactCashBalances f
			inner join master.DimAccount a on f.SK_AccountID = a.SK_AccountID
			inner join master.DimCustomer c on f.SK_CustomerID = c.SK_CustomerID
			inner join master.DimBroker b on a.SK_BrokerID = b.SK_BrokerID
			inner join master.DimDate d on f.SK_DateID = d.SK_DateID
     union select 'FactHoldings' as MessageSource, 'Row count joined' as MessageText, 
			count(*) as MessageData 
			from master.FactHoldings f
			inner join master.DimAccount a on f.SK_AccountID = a.SK_AccountID
			inner join master.DimCustomer c on f.SK_CustomerID = c.SK_CustomerID
			inner join master.DimBroker b on a.SK_BrokerID = b.SK_BrokerID
			inner join master.DimDate d on f.SK_DateID = d.SK_DateID
			inner join master.DimTime t on f.SK_TimeID = t.SK_TimeID
			inner join master.DimCompany m on f.SK_CompanyID = m.SK_CompanyID
			inner join master.DimSecurity s on f.SK_SecurityID = s.SK_SecurityID
    union select 'FactMarketHistory' as MessageSource, 'Row count joined' as MessageText, 
			count(*) as MessageData 
			from master.FactMarketHistory f
			inner join master.DimDate d on f.SK_DateID = d.SK_DateID
			inner join master.DimCompany m on f.SK_CompanyID = m.SK_CompanyID
			inner join master.DimSecurity s on f.SK_SecurityID = s.SK_SecurityID
    union select 'FactWatches' as MessageSource, 'Row count joined' as MessageText, 
			count(*) as MessageData 
			from master.FactWatches f
			inner join master.DimCustomer c on f.SK_CustomerID = c.SK_CustomerID
			inner join master.DimDate dp on f.SK_DateID_DatePlaced = dp.SK_DateID
			-- (cannot join on SK_DateID_DateRemoved because that field can be null)
			inner join master.DimSecurity s on f.SK_SecurityID = s.SK_SecurityID
    /* Additional information used at Audit time */
    union select 'master.DimCustomer' as MessageSource, 'Inactive customers' as MessageText, count(*) from master.DimCustomer where IsCurrent = True and Status = 'Inactive'
    union select 'FactWatches' as MessageSource, 'Inactive watches' as MessageText, count(*) from master.FactWatches where SK_DATEID_DATEREMOVED is not null
) y on 1=1
; 
/* Phase complete record */
insert into master.Dimessages
select
     MessageDateAndTime
     ,case when BatchID is null then 0 else BatchID end as BatchID
     ,MessageSource
     ,MessageText 
     ,MessageType
     ,MessageData
from (
     select CURRENT_TIMESTAMP as MessageDateAndTime
            ,max(BatchID) as BatchID
            ,'Phase Complete Record' as MessageSource
            ,'Batch Complete' as MessageText
            ,'PCR' as MessageType
            ,NULL as MessageData
  from master.Dimessages);