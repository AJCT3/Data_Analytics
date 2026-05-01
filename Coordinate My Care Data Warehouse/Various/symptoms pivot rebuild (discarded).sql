	
	--exec dbo.PivotSymptoms
	
	
	
	if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_SymptomsPre]') is not null
 	drop table [ETL_Local_PROD].[dbo].[AT_SymptomsPre]
	select *
	into [ETL_Local_PROD].[dbo].[AT_SymptomsPre]
	from 
	(
  select cps.cmc_id as #CMCId, Symptom, 'In case of ' +isnull(sc.Description,'(symptom not on picklist)') as NoteText
   from CarePlanSymptoms cps join PatientDetail pd on cps.CarePlan = pd.#Care_Plan
   left join ETL_PROD.dbo.Coded_Symptom sc with (nolock) on Symptom = sc.Code
   union all
   select cps.cmc_id as #CMCId, Symptom + '_DET', NoteText
   from CarePlanSymptoms cps join PatientDetail pd on cps.CarePlan = pd.#Care_Plan
   )d


   	if OBJECT_ID ('[ETL_Local_PROD].[dbo].[AT_SymptomsPre_AllData]') is not null
 	drop table [ETL_Local_PROD].[dbo].[AT_SymptomsPre_AllData]
	select
	*
	into [ETL_Local_PROD].[dbo].[AT_SymptomsPre_AllData]
	from
	
	(
   select replace(replace(stuff((
    select * from
--Generalise  to include all symptoms on the picklist MS 20.3.16
   (select distinct code as Symptom
    from etl_PROD.dbo.coded_symptom  with (nolock)
    union all
    select distinct code+'_DET'
    from etl_PROD.dbo.coded_symptom  with (nolock)) sel1
    order by Symptom
    for xml path('')),1,0,''),
    '<symptom>','['),'</symptom>','],')+'[#dummy#]'   as Symptom
	)f

--drop table   #Symptoms


	declare @sql nvarchar(max);
set @sql=
  N'DROP TABLE dbo.Symptoms;
  select * into dbo.Symptoms from
  (select * from [ETL_Local_PROD].[dbo].[AT_SymptomsPre] ) as source
  pivot
  (
  min([NoteText]) for Symptom in (' + 
   (
   
   select Symptom
    from[ETL_Local_PROD].[dbo].[AT_SymptomsPre_AllData] 
	)+')
  ) as AllData';
--print @sql;

execute (@sql);


drop table [ETL_Local_PROD].[dbo].[AT_SymptomsPre_AllData]
drop table [ETL_Local_PROD].[dbo].[AT_SymptomsPre]
