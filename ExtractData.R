library(data.table)
library(tidyverse)
library(tidymodels)


library(usethis)
use_git_config(user.name = "NikitaDogra", user.email = "nikita.dogra@mulsanneinsurance.com")


sqlserverconnectionstring <- "SERVER=ccgehsql04;DATABASE=QuotesEngine;UID=reserving;PWD=1q2w3e4rQWE123;"


# prior claims ------------------------------------------------------------


accidents_sql <- RxSqlServerData(sqlQuery =  "with 

exposure as (SELECT *  from QuotesEngine.[dbo].[PricingData] PD 
                                                       
              where pd.UW_dt  BETWEEN DATEADD(WEEK,-52,GETDATE()) AND GETDATE()
              and TransactionType in ('NB','RC')
              and Business not in ('Short-Term')),
              
  accidents1 as (
  SELECT  
  acc.PolicyNo, concat(
  
  case 

when acc.description like '%Accident%' then 'Accident'
when acc.description like '%Collision%' then 'Accident'
when acc.description like '%hit%' then 'Accident'
when acc.description like '%theft%' then 'Theft'
when acc.description like '%parked%' then 'Other'
when acc.description like '%Riot%' then 'Vandalism'
when acc.description like '%Windscreen/Glass%' then 'Glass'
when acc.description like '%Unknown%' then 'Other'
else acc.description 

end  
                                 ,'_',acc.AtFault) as ClaimType, acc.[Date]    from QuotesEngine.[dbo].[Accidents] acc
  where acc.PolicyNo in (select distinct PolicyNo from exposure)
  
              )
              

select * from accidents1 

", connectionString = sqlserverconnectionstring)

accidents <- rxDataStep(inData = accidents_sql, maxRowsByCols = -1)

accidents <- setDT(accidents)

# prior convictions -------------------------------------------------------

conv_sql <- RxSqlServerData(sqlQuery =  "
  with 

exposure as (SELECT *  from QuotesEngine.[dbo].[PricingData] PD 
                                                       
              where pd.UW_dt  BETWEEN DATEADD(WEEK,-52,GETDATE()) AND GETDATE()
              and TransactionType in ('NB','RC')
              and Business not in ('Short-Term')),
              
conv1 as (
SELECT  
acc.PolicyNo
, acc.ConvDate
, SUBSTRING(acc.AbiCode,1,2) AbiCode

  from QuotesEngine.[dbo].[Convictions] acc
  where acc.PolicyNo in (select distinct PolicyNo from exposure)
              
              )
              

select * from conv1 


", connectionString = sqlserverconnectionstring)

convictions <- rxDataStep(inData = conv_sql, maxRowsByCols = -1)


convictions <- setDT(convictions)

# Exposure ----------------------------------------------------------------


exp_sql <- RxSqlServerData(sqlQuery =  "
SELECT *  

from QuotesEngine.[dbo].[PricingData] PD 
                                             
  where pd.UW_dt  BETWEEN DATEADD(WEEK,-52,GETDATE()) AND GETDATE()
  and TransactionType in ('NB','RC')
  and Business not in ('Short-Term')", 
                           connectionString = sqlserverconnectionstring)

exposure <- rxDataStep(inData = exp_sql, maxRowsByCols = -1)

exposure <- setDT(exposure)

# join up to claims data --------------------------------------------------

pols <- unique(exposure[,.(PolicyNo,UW_dt)])
pols[,MaxDate := as.Date(UW_dt,"%Y-%m-%d")  - 365.25*3] # 5yrs back 

accidents2 <- merge.data.table(accidents, pols, by.x = "PolicyNo",by.y = "PolicyNo", all.x = T)
accidents2[,Date := as.Date(Date,"%d/%m/%Y")]
accidents2 <- accidents2[Date > MaxDate & Date <= UW_dt,]
accidents3 <- accidents2[,.(NrClaims = .N),by=.(PolicyNo,UW_dt,ClaimType)]
accidents4 <- dcast(accidents3, PolicyNo + UW_dt ~ ClaimType, value.var = "NrClaims")

exposure <- merge.data.table(exposure, accidents4, by.x = c("PolicyNo","UW_dt"),
                             by.y = c("PolicyNo","UW_dt"),
                             all.x = T)

rm(accidents,accidents2,accidents3, accidents4)
gc()



# join to convictions data  -----------------------------------------------

convictions2 <- merge.data.table(convictions, pols, by.x = "PolicyNo",by.y = "PolicyNo", all.x = T)
convictions2[,ConvDate := as.Date(ConvDate,"%d/%m/%Y")]
convictions2 <- convictions2[ConvDate > MaxDate & ConvDate <= UW_dt,]
convictions3 <- convictions2[,.(NrConv = .N),by=.(PolicyNo,UW_dt,AbiCode)]
convictions4 <- dcast(convictions3, PolicyNo + UW_dt ~ AbiCode, value.var = "NrConv")

exposure <- merge.data.table(exposure, convictions4, by.x = c("PolicyNo","UW_dt"),
                             by.y = c("PolicyNo","UW_dt"),
                             all.x = T)

rm(convictions,convictions2,convictions3, convictions4)
gc()



# add in features and engineering  ----------------------------------------


exposure[,UW_dt := as.Date(UW_dt,"%Y-%m-%d")]
exposure[, PurchaseDate := as.Date(PurchaseDate,"%d/%m/%Y")]
exposure[,YearsOwnedVehicle := as.numeric(UW_dt - PurchaseDate)/365.25]
exposure[,MaxPossibleNCD := floor(YearsOwnedVehicle)]
exposure[,NCD_Difference := MaxPossibleNCD - YearsNCDAllowed]
exposure[,D2_Older := lest::case_when(
  is.na(D2_Age) ~"Not Applicable",
  D2_Age > proposer_age ~ "Yes",
  TRUE ~ "No")]

exposure[,`Excess/Value` := Excess/Value]

exposure[,AgeGotLicense :=proposer_age - LICmnths_PPSR/12]
exposure[,Model2 := gsub( " .*$", "",Model)]


# add in all  the features Ian mentions he looks at here 

#PC high annual Mileage
exposure$"PC_annual_mileage" <- ifelse(exposure$Business=="P/CAR" & exposure$AnnualMileage>20000,TRUE,FALSE)

#CV Courier and low annual mileage
exposure$"CV_Courier_Low_Mileage" <- ifelse(exposure$Business=="CV" & exposure$AnnualMileage<12000 & exposure$OCC_DESC_PPSR %in% c(
  
  "Courier"  
  ,"Courier - Parcel Delivery"
  , "Delivery Courier" 
  , "Delivery Rounds Person"
  , "Courier - Driver" 
  , "Delivery Driver"),TRUE,FALSE)

#CV vehicles Garaged 
exposure$"CV_Veh_Garaged" <- ifelse(exposure$Business=="CV" & exposure$Veh_Stor=="Garage",TRUE,FALSE)

#Young but retired 
exposure$"Young_But_Retired" <- ifelse(exposure$OCC_DESC_PPSR=="Retired" & exposure$proposer_age<50,TRUE,FALSE)

#House Husbands
exposure$"House Husband" <- ifelse(exposure$Sex_PPSR=="M" & exposure$OCC_DESC_PPSR %in% c("Househusband" ,"Houseman or Woman" 
                                                                                          ,"House Parent" 
                                                                                          , "Housekeeper" 
                                                                                          , "Houseperson (Housewife or Househusband)"),TRUE,FALSE)

#Fronting
exposure$"Fronting" <- ifelse(exposure$D2_Age<30 & (exposure$proposer_age-exposure$D2_Age)>=20,TRUE,FALSE)



exposure %>% left_join(exposure %>% 
                         group_by(veh_age) %>% 
                         summarise(Value_50th_perc = quantile(Value,0.5),
                                   Value_25th_perc = quantile(Value,0.25),
                                   Value_75th_perc = quantile(Value,0.75)),
                                   by = "veh_age") %>% glimpse()

#Low value new vehicles
exposure$"Low_Value_But_New" <- ifelse(exposure$veh_age < 10 & (exposure$Value < exposure$Value_25th_perc | exposure$Value > exposure$Value_75th_perc) , TRUE,FALSE)


#Vehcile ownership with no ncd or claims
exposure$"Owned_But_no_ncd_or_claims" <- ifelse(exposure$owner=="Proposer/Policyholder" & exposure$YearsNcd == 0 & exposure$CLM_CNT ==0 , TRUE,FALSE)

#International licences with residency over 12 months


#Low declared annual mileages, particularly when not SD&P only use.



exposure %>% 
  as_tibble() %>% 
  mutate(across(Accident_N:Z0,
                .fns = ~ifelse(is.na(.x),0,.x))) -> exposure


exposure %>% 
  mutate(NrOfRiskIndicatorsFound = PC_annual_mileage + CV_Courier_Low_Mileage + 
           CV_Veh_Garaged + Young_But_Retired + 
           `House Husband` + Fronting+ Low_Value_But_New) -> exposure 


summarise(across(c(PC_annual_mileage:Low_Value_But_New),function(x){sum(x)}))

# Modeling ----------------------------------------------------------------
library(readxl)
BadPolicyTable <- read_excel("R:/Share/Projects/ErrorCheckig/Post Bind Report.xls")
badpolicies <- sub(".*No:","",BadPolicyTable$"Reference")
badpolicies<-trimws(badpolicies)


# create a new dataframe to store all the bad policies

 exposure %>% filter(PolicyNo %in% badpolicies) -> bad_policies 

 `%notin%` <- Negate(`%in%`)
 
# exposure <- exposure %>% filter(PolicyNo %notin% badpolicies)


# split the data into training/holdout 
training <- e=xposure %>% filter(UW_dt <= Sys.Date()-14)
holdout <- exposure %>% filter(UW_dt > Sys.Date()-14)



# recipe for modeling 
isbad_fn <- function(x){ifelse(is.na(x),1,0)}



rec <- recipe(GWP ~ 
                Business +
                Supergroup + 
                Cover + 
                Pay_Desc +
                posttown + 
                PC_Region +
                PC_Town + 
                proposer_age + 
                youngest_age + 
                D2_Age + 
                LIC_TYPE_PPSR + 
                LICmnths_PPSR + 
                Sex_PPSR + 
                Make + 
                Model2 +
                YearsNCDAllowed + 
                YearsOwnedVehicle + 
                Excess + 
                OCC_DESC_PPSR + 
                BUS_TYPE_D1 + 
                LIC_TYPE_D2 + 
                Sex_D2 + 
                veh_age + 
                veh_classuse + 
                driver_cnt +
                perm_driv + 
                AnnualMileage + 
                EngineCC + 
                Value + 
                Veh_Stor + 
                RHD_LHD + 
                owner + 
                Homeowner + 
                Number_Other_VehDriven + 
                NoSeats + 
                Accident_N+ 
                Accident_Y +
                Fire_N + 
                Fire_Y + 
                Flood_Y + 
                Glass_N + 
                Glass_Y + 
                Other_N + 
                Other_Y + 
                Storm_Y + 
                Theft_N + 
                Theft_Y + 
                Vandalism_N + 
                Vandalism_Y + 
                AgeGotLicense,
              data = training
                ) %>% 
  step_mutate_at(all_numeric(),
                fn = list(isBad = isbad_fn)) %>%
  step_meanimpute(all_numeric()) %>% 
  step_modeimpute(all_nominal()) %>% 
  step_mutate(GWP_per_Value = GWP/Value,
              GWP_per_age = GWP/proposer_age,
              GWP_per_veh_age = GWP/veh_age,
              
              GWP_per_youngest_age = GWP/youngest_age,
              AgeDiff = proposer_age - youngest_age) %>% 
  step_other(all_nominal(), threshold = 0.01) %>% 
  prep()

training_prepped <- bake(rec, new_data = training)
holdout_prepped <- bake(rec, new_data = holdout)

library(h2o)
h2o.init()

training.h <- as.h2o(training_prepped)
holdout.h <- as.h2o(holdout_prepped)


isoforst <- h2o.isolationForest(
  training_frame = training.h,
  ntrees = 300,
  max_depth = 15,
  min_rows = 5
)

preds <- h2o.predict(isoforst, newdata = holdout.h,use_datatable  = T)

as.data.frame(preds)$predict -> estimated

holdout$Estimated <- estimated


holdout %>%
  mutate(AtRisk = ifelse(Estimated > quantile(Estimated,0.99),1,0)) -> holdout


holdout %>% 
  filter(AtRisk > 0) %>% 
  select(PolicyNo,
           Business,
           Cover,
           Pay_Desc,
           PC_Region,
           PC_Town,
           proposer_age,
           youngest_age,
           D2_Age,
           LIC_TYPE_PPSR:OCC_DESC_PPSR,
           Date_resident_D1:Sex_D2,
           veh_age:Value,
           Veh_Stor:GWP,
           Supergroup:Other_Y) %>% 
  View()
  fwrite(.,"Sample1.csv")

# holdout.h$Anomaly[holdout.h$PolicyNo == "3216150001556"] <- 1

# find the position of the specific one 
which(holdout$AtRisk > 0)




holdout_prepped$AtRisk <- "N"
holdout_prepped$AtRisk[which(holdout$AtRisk > 0)] <- "Y"


holdout %>% filter(AtRisk == 1) %>% View()

# 
# 
# # local model 
# vars <- setdiff(colnames(holdout.h2),"AtRisk")
# 
# local_rf <- h2o.randomForest(x=vars,
#                              y = "AtRisk",
#                              training_frame = holdout.h2,
#                              ntrees = 1,
#                              max_depth = 5)

# TODO: get a list of all problematic policies from ian and exclude those from the training data 
# TODO: get a list of manual checks Ian and the underwriters apply and include those as columns in the data 
# TODO: 

library(party)

party_model <- ctree(AtRisk ~ .,
                     data = holdout_prepped %>% mutate(AtRisk = as.factor(AtRisk)))

plot(party_model)


sum()
