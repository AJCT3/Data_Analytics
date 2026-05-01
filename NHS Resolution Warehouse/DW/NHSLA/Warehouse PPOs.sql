 
   if OBJECT_ID('[NHR Data Mart Test].[dbo].[Current_PPO]') is not null
 drop table [NHR Data Mart Test].[dbo].[Current_PPO]
 
 select
 Claimid
 into [NHR Data Mart Test].[dbo].[Current_PPO] 
 FROM [NHR Data Mart Test].[dbo].[ClaimGeneral]b
 where ClaimStatusTypeName in
 (
	'Periodical Payments',
	'Periodical Payments and Indemnity Given',
	'Periodical Payments with Reverse Indemnity'
	)