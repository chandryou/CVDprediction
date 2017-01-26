CREATE TABLE #Codesets (
  codeset_id int NOT NULL,
  concept_id bigint NOT NULL
)
;

INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 2 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @cdm_database_schema.CONCEPT where concept_id in (1332497,19122209,21601860,1545997,19123592,1545996,1545959,21601906,43534776,1586229,19075040,19075051,1586255,19075052,1539469,1526479,19112569,21601859,19077498,19077499,21601857,19019116,21601863,40165638,40165642,40165646,19022104,40175390,40175394,40175400,21601862,1510813,43534777,40165245,40165253,40165261,21601856,1539463,1539411,1539407,19023563,21601901,43534775)and invalid_reason is null

) I
) C;
INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 4 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @cdm_database_schema.CONCEPT where concept_id in (312327,434376,438170,444406)and invalid_reason is null

) I
) C;
INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 5 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @cdm_database_schema.CONCEPT where concept_id in (19022531,19047423,46275864,1112807,19103854,19021575,1113143,1112923,704946,19060437,1112896,1112900,19004043,19065472,19059056,40252639,1350310,1350311,1350332,1322184,21600989,19075601,21600992,19076623,21601004,40163720,40241188,21600990,1302398,19079773,21601000,19042778,21601001,19042779)and invalid_reason is null

) I
) C;
INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 7 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @cdm_database_schema.CONCEPT where concept_id in (4110189,443454,4111714,4108356,4110190,4110192)and invalid_reason is null

) I
) C;
INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 8 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @cdm_database_schema.CONCEPT where concept_id in (4319328,4326561,4218781,376713,4176892,4110185,436430,4111709,4226021,432923,4108952,4111708,4049659)and invalid_reason is null

) I
) C;
INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 9 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @cdm_database_schema.CONCEPT where concept_id in (319835,316139,314378,439846)and invalid_reason is null

) I
) C;
INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 10 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @cdm_database_schema.CONCEPT where concept_id in (320128)and invalid_reason is null

) I
) C;
INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 11 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @cdm_database_schema.CONCEPT where concept_id in (2001510,4142645,4305852,4006788)and invalid_reason is null

) I
) C;
INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 12 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @cdm_database_schema.CONCEPT where concept_id in (4145739,4305699,4125350,4260263)and invalid_reason is null

) I
) C;
INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 13 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @cdm_database_schema.CONCEPT where concept_id in (4119951,45766075,4178129,4267568,44784623,4108670,4215140,4011131,319844,312327,44782769,44782712,45766115,434376,45766150,438438,4243372,4108669,4151046,4275436,438170,45771322,438447,441579,436706,4324413,4051874,4303359,4147223,4189939,4145721,4119944,4119456,4119945,4119946,4121466,4124685,4270024,319039,4119457,4119943,4121464,4121465,4124684,4119948,4126801,4296653,46270162,46270163,43020460,43020461,45766076,46270159,46270160,45766116,45766151,46270161,46273495,46270158,46270164,444406,4119947,43531588,315832,321318,4264145,4184827,4310270,4231426,4199962,4155963,4242670,315286,4185302,4178321,4069186,43022045,317576,46269996,4175846,42872402,4252385,45766238,4127089,4119613,4225958,4134723,40481919,4108172,44806109,316995,4097848,4243371,4108215,4108673,4116486,4215259,4110961,4094055,4096252,4145696,4219755,43020660,4185932,4092936,4153091,4048534,4155962,4209308,4173632,44783791,4323202,4155007,4329847,4170094,4154704,4186397,4173171,4119455,4262446,4200113,4168972,4119949,4121467,4119950,314666,4121468,4138833,4198141,4030582,4206867,4207921,4209541,315296,315830,4161973,4161457,4161456,4155009,4161974,4155008,4178622,4201629,4046022,4304192,4161455,45766117,4124686,4124683,4111393,4119942,4078531,4094054,44808600,4263712,4108217,4108677,4108218,45766241,45766114,45766113,45773170,4068938,4172865,4124682,439693,4324893,46274044)and invalid_reason is null

) I
) C;
INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 14 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @cdm_database_schema.CONCEPT where concept_id in (44797721,4249574,4138327,4172545,4048784,4045735,4031045,4045747,4112018,4215954,4319328,4111710,4103390,4211509,4048606,4201094,4326561,4111711,4047732,4218781,376713,4071589,4243309,42872427,4110189,443454,43530683,4111714,4108356,45772786,4110190,46273649,46270031,43531607,46270381,4110192,45767658,44782773,46270380,4173481,381316,4121664,43530851,4337830,438873,440531,434166,439932,440537,434756,4014781,4099974,46270111,46273491,45766120,4176892,4045734,45766068,4045741,4153352,4039439,4209442,4216353,4224614,4134421,4185701,4250507,4267862,4338502,4052462,4327350,4217146,4306943,4090122,4112019,435960,432475,434508,438279,442341,436258,439949,433051,252477,261433,381440,255105,256844,441432,256556,436557,4048133,4176148,4126076,4078314,4289598,4199890,4139778,42873045,42873044,380113,42873046,42873123,4046362,40479572,4043731,4131383,4046237,4119140,4178726,4080892,4345688,4110185,4110186,42872434,4242720,439847,42873157,444197,440561,444198,444196,434785,433623,439950,433624,433052,441158,435672,436526,437106,436841,4009796,438596,432478,433058,440869,436842,432764,4071732,4025201,4328027,40492969,4299377,4048277,4048278,434155,4048279,4180743,4079972,4310996,43530669,43530670,4219010,4045740,4045746,4046360,4112022,4141405,4045744,4047731,4045743,4147995,4077819,4046090,379778,444091,443790,443864,377254,4145897,437388,4129290,4159152,45766830,45766118,4006295,436430,4085234,4144154,4106058,44782730,46271801,43530671,4111709,4077086,4180610,4323618,43531605,4189462,435378,432743,433037,435938,435931,433882,4017105,4023571,4151359,4046359,45766119,4174299,436519,4171122,260841,4079973,4171123,4173332,4049750,4137761,4110676,4229432,4174848,4263703,4129534,4129535,4319146,4043732,4045748,4301259,4162754,4238315,4045737,4046361,4045738,4110196,4146185,380943,4079430,4079433,4082161,4079431,4079432,4079434,4082162,4111707,4120104,4079120,4079021,4082163,43530674,43530727,4148906,4168056,4046363,43530728,432923,4071066,4046365,4046364,441709,435964,373057,440244,440251,438894,439171,435104,435959,434789,440250,438003,439182,439944,435390,437413,4077828,4077958,4077201,4078448,4108952,4078446,4077827,4077200,4077959,4078447,4111708,45773167,4049659,4318408,439040,4130539,4095178,432752,433050,439194,441985,436553,432474,440870,440236,373347,433899,437133,438595,435391,438285,4183593,42873042,40480273,4045745,4142739,4159140,4046358,4136545,4201028,4196276,4134162,4154699,4136546,4201635,4196275,4017107,4046089,443752,4086178)and invalid_reason is null

) I
) C;

select row_number() over (order by P.person_id, P.start_date) as event_id, P.person_id, P.start_date, P.end_date, OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date
INTO #primary_events
FROM
(
  select P.person_id, P.start_date, P.end_date, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY start_date ASC) ordinal
  FROM 
  (
  select C.person_id, C.observation_period_start_date as start_date, C.observation_period_end_date as end_date, C.period_type_concept_id as TARGET_CONCEPT_ID
from 
(
        select op.*, ROW_NUMBER() over (PARTITION BY op.person_id ORDER BY op.observation_period_start_date) as ordinal
        FROM @cdm_database_schema.OBSERVATION_PERIOD op
) C
JOIN @cdm_database_schema.PERSON P on C.person_id = P.person_id
WHERE C.observation_period_start_date <= DATEFROMPARTS(2002, 01, 29)
AND C.observation_period_end_date >= DATEFROMPARTS(2008, 11, 30)
AND (YEAR(C.observation_period_start_date) - P.year_of_birth >= 30 and YEAR(C.observation_period_start_date) - P.year_of_birth <= 80)

  ) P
) P
JOIN @cdm_database_schema.observation_period OP on P.person_id = OP.person_id and P.start_date between OP.observation_period_start_date and op.observation_period_end_date
WHERE DATEADD(day,0,OP.OBSERVATION_PERIOD_START_DATE) <= P.START_DATE AND DATEADD(day,0,P.START_DATE) <= OP.OBSERVATION_PERIOD_END_DATE AND P.ordinal = 1
;


SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date
INTO #qualified_events
FROM 
(
  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() over (partition by pe.person_id order by pe.start_date ASC) as ordinal
  FROM #primary_events pe
  
) QE

;


create table #inclusionRuleCohorts 
(
  inclusion_rule_id bigint,
  event_id bigint
)
;
INSERT INTO #inclusionRuleCohorts (inclusion_rule_id, event_id)
select 0 as inclusion_rule_id, event_id
FROM 
(
  select pe.event_id
  FROM #qualified_events pe
  
JOIN (
select 0 as index_id, event_id
FROM
(
  select event_id FROM
  (
    SELECT 0 as index_id, p.event_id
FROM #qualified_events P
LEFT JOIN
(
  select C.person_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, DATEADD(day,1,C.condition_start_date)) as end_date, C.CONDITION_CONCEPT_ID as TARGET_CONCEPT_ID
from 
(
        select co.*, ROW_NUMBER() over (PARTITION BY co.person_id ORDER BY co.condition_start_date) as ordinal
        FROM @cdm_database_schema.CONDITION_OCCURRENCE co
where co.condition_concept_id in (SELECT concept_id from  #Codesets where codeset_id = 13)
) C



) A on A.person_id = P.person_id and A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE and A.START_DATE <= DATEADD(day,2555,P.START_DATE)
GROUP BY p.event_id
HAVING COUNT(A.TARGET_CONCEPT_ID) <= 0


  ) CQ
  GROUP BY event_id
  HAVING COUNT(index_id) = 1
) G
) AC on AC.event_id = pe.event_id
) Results
;

INSERT INTO #inclusionRuleCohorts (inclusion_rule_id, event_id)
select 1 as inclusion_rule_id, event_id
FROM 
(
  select pe.event_id
  FROM #qualified_events pe
  
JOIN (
select 0 as index_id, event_id
FROM
(
  select event_id FROM
  (
    SELECT 0 as index_id, p.event_id
FROM #qualified_events P
LEFT JOIN
(
  select C.person_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, DATEADD(day,1,C.condition_start_date)) as end_date, C.CONDITION_CONCEPT_ID as TARGET_CONCEPT_ID
from 
(
        select co.*, ROW_NUMBER() over (PARTITION BY co.person_id ORDER BY co.condition_start_date) as ordinal
        FROM @cdm_database_schema.CONDITION_OCCURRENCE co
where co.condition_concept_id in (SELECT concept_id from  #Codesets where codeset_id = 14)
) C



) A on A.person_id = P.person_id and A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE and A.START_DATE <= DATEADD(day,2555,P.START_DATE)
GROUP BY p.event_id
HAVING COUNT(A.TARGET_CONCEPT_ID) <= 0


  ) CQ
  GROUP BY event_id
  HAVING COUNT(index_id) = 1
) G
) AC on AC.event_id = pe.event_id
) Results
;

INSERT INTO #inclusionRuleCohorts (inclusion_rule_id, event_id)
select 2 as inclusion_rule_id, event_id
FROM 
(
  select pe.event_id
  FROM #qualified_events pe
  
JOIN (
select 0 as index_id, event_id
FROM
(
  select event_id FROM
  (
    SELECT 0 as index_id, p.event_id
FROM #qualified_events P
LEFT JOIN
(
  select C.person_id, C.procedure_date as start_date, DATEADD(d,1,C.procedure_date) as END_DATE, C.procedure_concept_id as TARGET_CONCEPT_ID
from 
(
  select po.*, ROW_NUMBER() over (PARTITION BY po.person_id ORDER BY po.procedure_date) as ordinal
  FROM @cdm_database_schema.PROCEDURE_OCCURRENCE po
where po.procedure_concept_id in (SELECT concept_id from  #Codesets where codeset_id = 12)
) C



) A on A.person_id = P.person_id and A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE and A.START_DATE <= DATEADD(day,2555,P.START_DATE)
GROUP BY p.event_id
HAVING COUNT(A.TARGET_CONCEPT_ID) <= 0


  ) CQ
  GROUP BY event_id
  HAVING COUNT(index_id) = 1
) G
) AC on AC.event_id = pe.event_id
) Results
;

INSERT INTO #inclusionRuleCohorts (inclusion_rule_id, event_id)
select 3 as inclusion_rule_id, event_id
FROM 
(
  select pe.event_id
  FROM #qualified_events pe
  
JOIN (
select 0 as index_id, event_id
FROM
(
  select event_id FROM
  (
    SELECT 0 as index_id, p.event_id
FROM #qualified_events P
LEFT JOIN
(
  select C.person_id, C.procedure_date as start_date, DATEADD(d,1,C.procedure_date) as END_DATE, C.procedure_concept_id as TARGET_CONCEPT_ID
from 
(
  select po.*, ROW_NUMBER() over (PARTITION BY po.person_id ORDER BY po.procedure_date) as ordinal
  FROM @cdm_database_schema.PROCEDURE_OCCURRENCE po
where po.procedure_concept_id in (SELECT concept_id from  #Codesets where codeset_id = 11)
) C



) A on A.person_id = P.person_id and A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE and A.START_DATE <= DATEADD(day,2555,P.START_DATE)
GROUP BY p.event_id
HAVING COUNT(A.TARGET_CONCEPT_ID) <= 0


  ) CQ
  GROUP BY event_id
  HAVING COUNT(index_id) = 1
) G
) AC on AC.event_id = pe.event_id
) Results
;


with cteIncludedEvents(event_id, person_id, start_date, end_date, op_start_date, op_end_date, ordinal) as
(
  SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, row_number() over (partition by person_id order by start_date ASC) as ordinal
  from
  (
    select Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date, SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) as inclusion_rule_mask
    from #qualified_events Q
    LEFT JOIN #inclusionRuleCohorts I on I.event_id = Q.event_id
    GROUP BY Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date
  ) MG -- matching groups

  -- the matching group with all bits set ( POWER(2,# of inclusion rules) - 1 = inclusion_rule_mask
  WHERE (MG.inclusion_rule_mask = POWER(cast(2 as bigint),4)-1)

)
select event_id, person_id, start_date, end_date, op_start_date, op_end_date
into #included_events
FROM cteIncludedEvents Results
WHERE Results.ordinal = 1
;

-- Apply end date stratagies
-- by default, all events extend to the op_end_date.
select event_id, person_id, op_end_date as end_date
into #cohort_ends
from #included_events;



DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select @target_cohort_id as cohort_definition_id, F.person_id, F.start_date, F.end_date
FROM (
  select Q.person_id, Q.start_date, E.end_date, row_number() over (partition by Q.event_id order by E.end_date) as ordinal 
  from #qualified_events Q
  join #cohort_ends E on Q.event_id = E.event_id and Q.person_id = E.person_id and E.end_date >= Q.start_date
) F
WHERE F.ordinal = 1
;




TRUNCATE TABLE #cohort_ends;
DROP TABLE #cohort_ends;

TRUNCATE TABLE #inclusionRuleCohorts;
DROP TABLE #inclusionRuleCohorts;

TRUNCATE TABLE #qualified_events;
DROP TABLE #qualified_events;

TRUNCATE TABLE #included_events;
DROP TABLE #included_events;

TRUNCATE TABLE #primary_events;
DROP TABLE #primary_events;

TRUNCATE TABLE #Codesets;
DROP TABLE #Codesets;