DROP TABLE IF EXISTS @synthea_schema.allergies;
CREATE TABLE @synthea_schema.allergies (
"start"        date,
stop         date,
patient      varchar(50),
encounter    varchar(50),
code         varchar(50),
system       varchar(50),
description  varchar(255),
"type"       varchar(255),
category     varchar(255),
reaction1    varchar(255),
description1 varchar(255),
severity1    varchar(255),
reaction2    varchar(255),
description2 varchar(255),
severity2    varchar(255)
);

DROP TABLE IF EXISTS @synthea_schema.careplans;
CREATE TABLE @synthea_schema.careplans (
id            varchar(50),
"start"         date,
stop          date,
patient       varchar(50),
encounter     varchar(50),
code          varchar(50),
description   varchar(255),
reasoncode   varchar(255),
reasondescription   varchar(255)
);

DROP TABLE IF EXISTS @synthea_schema.conditions;
CREATE TABLE @synthea_schema.conditions (
"start"         date,
stop          date,
patient       varchar(50),
encounter     varchar(50),
system        varchar(50),
code          varchar(50),
description   varchar(255)
);

DROP TABLE IF EXISTS @synthea_schema.encounters;
CREATE TABLE @synthea_schema.encounters (
id            		varchar(50),
"start"         		date,
stop							date,
patient       		varchar(50),
organization   		varchar(50),
provider			varchar(50),
payer			varchar(50),
encounterclass		varchar(50),
code          		varchar(50),
description   		varchar(255),
base_encounter_cost numeric,
total_claim_cost		numeric,
payer_coverage		numeric,
reasoncode   			varchar(100),
reasondescription varchar(255)
);

DROP TABLE IF EXISTS @synthea_schema.immunizations;
CREATE TABLE @synthea_schema.immunizations (
"date"        date,
patient       varchar(50),
encounter     varchar(50),
code          varchar(50),
description   varchar(255),
base_cost	numeric
);

DROP TABLE IF EXISTS @synthea_schema.imaging_studies;
CREATE TABLE @synthea_schema.imaging_studies (
id			  varchar(50),
"date"        date,
patient					varchar(50),
encounter				varchar(50),
series_uid			varchar(255),
bodysite_code			varchar(50),
bodysite_description		varchar(255),
modality_code			varchar(50),
modality_description		varchar(255),
instance_uid			varchar(255),
SOP_code					varchar(50),
SOP_description			varchar(255),
procedure_code			varchar(50)
);

DROP TABLE IF EXISTS @synthea_schema.medications;
CREATE TABLE @synthea_schema.medications (
"start"         date,
stop          date,
patient       varchar(50),
payer		varchar(50),
encounter     varchar(50),
code          varchar(50),
description   varchar(255),
base_cost	  numeric,
payer_coverage		numeric,
dispenses			int,
totalcost			numeric,
reasoncode   	varchar(50),
reasondescription   varchar(255)
);

DROP TABLE IF EXISTS @synthea_schema.observations;
CREATE TABLE @synthea_schema.observations (
"date"         date,
patient       varchar(50),
encounter     varchar(50),
category      varchar(50),
code          varchar(50),
description   varchar(255),
value     		varchar(255),
units         varchar(50),
"type"		  	varchar(50)
);

DROP TABLE IF EXISTS @synthea_schema.organizations;
CREATE TABLE @synthea_schema.organizations (
id			  varchar(50),
"name"	      varchar(255),
address       varchar(255),
city		  varchar(50),
state     	  varchar(50),
zip           varchar(50),
lat		numeric,
lon 		numeric,
phone		  varchar(50),
revenue		numeric,
utilization	  varchar(50)
);

DROP TABLE IF EXISTS @synthea_schema.patients;
CREATE TABLE @synthea_schema.patients (
id            varchar(50),
birthdate     date,
deathdate     date,
ssn           varchar(50),
drivers       varchar(50),
passport      varchar(50),
prefix        varchar(20),
first         varchar(50),
middle        varchar(50),
last          varchar(50),
suffix        varchar(20),
maiden        varchar(50),
marital       varchar(20),
race          varchar(20),
ethnicity     varchar(20),
gender        varchar(20),
birthplace    varchar(100),
address       varchar(100),
city					varchar(50),
state					varchar(50),
county		varchar(50),
fips varchar(50),
zip						varchar(20),
lat		numeric,
lon		numeric,
healthcare_expenses	numeric,
healthcare_coverage	numeric,
income int
);

DROP TABLE IF EXISTS @synthea_schema.procedures;
CREATE TABLE @synthea_schema.procedures (
"start"         date,
stop          date,
patient       varchar(50),
encounter     varchar(50),
system        varchar(50),
code          varchar(50),
description   varchar(255),
base_cost		numeric,
reasoncode	varchar(50),
reasondescription	varchar(255)
);

DROP TABLE IF EXISTS @synthea_schema.providers;
CREATE TABLE @synthea_schema.providers (
id varchar(50),
organization varchar(50),
"name" varchar(100),
gender varchar(20),
speciality varchar(100),
address varchar(255),
city varchar(50),
state varchar(50),
zip varchar(50),
lat numeric,
lon numeric,
encounters int,
"procedures" int
);

DROP TABLE IF EXISTS @synthea_schema.devices;
CREATE TABLE @synthea_schema.devices (
"start"         date,
stop          date,
patient       varchar(50),
encounter     varchar(50),
code          varchar(50),
description   varchar(255),
udi           varchar(255)
);

DROP TABLE IF EXISTS @synthea_schema.claims;
CREATE TABLE @synthea_schema.claims (
  id                           varchar(50),
  patientid                    varchar(50),
  providerid                   varchar(50),
  primarypatientinsuranceid    varchar(50),
  secondarypatientinsuranceid  varchar(50),
  departmentid                 varchar(50),
  patientdepartmentid          varchar(50),
  diagnosis1                   varchar(50),
  diagnosis2                   varchar(50),
  diagnosis3                   varchar(50),
  diagnosis4                   varchar(50),
  diagnosis5                   varchar(50),
  diagnosis6                   varchar(50),
  diagnosis7                   varchar(50),
  diagnosis8                   varchar(50),
  referringproviderid          varchar(50),
  appointmentid                varchar(50),
  currentillnessdate           date,
  servicedate                  date,
  supervisingproviderid        varchar(50),
  status1                      varchar(50),
  status2                      varchar(50),
  statusp                      varchar(50),
  outstanding1                 numeric,
  outstanding2                 numeric,
  outstandingp                 numeric,
  lastbilleddate1              date,
  lastbilleddate2              date,
  lastbilleddatep              date,
  healthcareclaimtypeid1       numeric,
  healthcareclaimtypeid2       numeric
);

DROP TABLE IF EXISTS @synthea_schema.claims_transactions;
CREATE TABLE @synthea_schema.claims_transactions (
  id                     varchar(50),
  claimid                varchar(50),
  chargeid               numeric,
  patientid              varchar(50),
  "type"                 varchar(50),
  amount                 numeric,
  method                 varchar(50),
  fromdate               date,
  todate                 date,
  placeofservice         varchar(50),
  procedurecode          varchar(50),
  modifier1              varchar(50),
  modifier2              varchar(50),
  diagnosisref1          numeric,
  diagnosisref2          numeric,
  diagnosisref3          numeric,
  diagnosisref4          numeric,
  units                  numeric,
  departmentid           numeric,
  notes                  varchar(255),
  unitamount             numeric,
  transferoutid          numeric,
  transfertype           varchar(50),
  payments               numeric,
  adjustments            numeric,
  transfers              numeric,
  outstanding            numeric,
  appointmentid          varchar(50),
  linenote               varchar(255),
  patientinsuranceid     varchar(50),
  feescheduleid          numeric,
  providerid             varchar(50),
  supervisingproviderid  varchar(50)
);

DROP TABLE IF EXISTS @synthea_schema.payer_transitions;
CREATE TABLE @synthea_schema.payer_transitions (
  patient           varchar(50),
  memberid         varchar(50),
  start_date       date,
  end_date         date,
  payer            varchar(50),
  secondary_payer  varchar(50),
  plan_ownership        varchar(50),
  owner_name       varchar(100)
);

DROP TABLE IF EXISTS @synthea_schema.payers;
CREATE TABLE @synthea_schema.payers (
  id                       varchar(50),
  "name"                     varchar(100),
  ownership                varchar NULL,
  address                  varchar(100),
  city                     varchar(50),
  state_headquartered      varchar(50),
  zip                      varchar(20),
  phone                    varchar(50),
  amount_covered           numeric,
  amount_uncovered         numeric,
  revenue                  numeric,
  covered_encounters       numeric,
  uncovered_encounters     numeric,
  covered_medications      numeric,
  uncovered_medications    numeric,
  covered_procedures       numeric,
  uncovered_procedures     numeric,
  covered_immunizations    numeric,
  uncovered_immunizations  numeric,
  unique_customers         numeric,
  qols_avg                 numeric,
  member_months            numeric
);

DROP TABLE IF EXISTS @synthea_schema.supplies;
CREATE TABLE @synthea_schema.supplies (
  "date"       date,
  patient      varchar(50),
  encounter    varchar(50),
  code         varchar(50),
  description  varchar(255),
  quantity     numeric
);