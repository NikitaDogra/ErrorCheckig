library(data.table)


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
exposure[,D2_Older := fcase(is.na(D2_Age),"Not Applicable",
                            D2_Age > proposer_age, "Yes",
                            default = "No")]
exposure[,`Excess/Value` := Excess/Value]

exposure[,AgeGotLicense :=proposer_age - LICmnths_PPSR/12]
exposure[,Model2 := gsub( " .*$", "",Model)]


exposure %>% 
  as_tibble() %>% 
  mutate(across(Accident_N:Z0,
                .fns = ~ifelse(is.na(.x),0,.x))) -> exposure

# Modeling ----------------------------------------------------------------

library(tidymodels)

# split the data into training/holdout 

training <- exposure %>% filter(UW_dt <= Sys.Date()-14)
holdout <- exposure %>% filter(UW_dt > Sys.Date()-14)



# recipe for modeling 

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
  step_meanimpute(all_numeric()) %>% 
  step_modeimpute(all_nominal()) %>% 
  step_mutate(GWP_per_Value = GWP/Value,
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

preds <- h2o.predict(isoforst, newdata = holdout.h)

as.data.frame(preds)$predict -> estimated

holdout$Estimated <- estimated


holdout %>%
  mutate(AtRisk = ifelse(Estimated > quantile(Estimated,0.99),1,0)) -> holdout


# holdout.h$Anomaly[holdout.h$PolicyNo == "3216150001556"] <- 1

# find the position of the specific one 
which(holdout$AtRisk > 0)




holdout_prepped$AtRisk <- "N"
holdout_prepped$AtRisk[which(holdout$AtRisk > 0)] <- "Y"

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

library(party)

party_model <- ctree(AtRisk ~ .,
                     data = holdout_prepped %>% mutate(AtRisk = as.factor(AtRisk)))

plot(party_model)


sum()
