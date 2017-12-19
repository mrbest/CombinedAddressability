options(scipen = 999)
source("sprklyRSpark.R")
library(readr)
library(ggplot2)
library(tictoc)
library(lucr)

process_one_contract <- function(bic_or_gsa, add_mode, contract_name)
{ 
  addressability_matrix <- dplyr_gen_addressability_matrix_df(add_mode, contract_name, training_transactions)
  addressability_matrix <- addressability_matrix %>% filter(is.na(product_or_service_code) == FALSE & is.na(naics_code) == FALSE)
  ##added the writing of contract solution
  date_path <- gsub("-", "", Sys.Date())
  dir.create(date_path)
  dir.create(paste0(date_path,"/matrices"))
  file_time_stamp <- gsub(" ", "", Sys.time())
  file_time_stamp <- gsub(":","", file_time_stamp)
  file_contract_name <- gsub("/", "", contract_name)
  file_contract_name <- gsub(" ", "_", file_contract_name)
  file_contract_name <- gsub("-", "", file_contract_name)
  write_csv(addressability_matrix, paste0(date_path,"/matrices/",file_contract_name,"_matrix_", bic_or_gsa,"_",add_mode,"_", file_time_stamp, ".csv"))
  #master_addressability_matrix <<- bind_rows(master_addressability_matrix, addressability_matrix)
  result_df <- dplyr_gen_testPhase_df(add_mode, addressability_matrix, testing_transactions, contract_name)
  
  addressability_result_row_count <- result_df %>% count()
  if(addressability_result_row_count >0 )
        {
        addressability_result <- result_df %>% select(dollars_obligated)%>% sum()
        }
  else
    {
      addressability_result <- 0
    }
  actual_obligations <<- opt_get_contract_totals(contract_name)
  addressability_result_formatted <- to_currency(addressability_result, currency_symbol = "$", symbol_first = TRUE, group_size = 3, group_delim = ",", decimal_size = 2,decimal_delim = ".")
  print(paste0( contract_name," addressable spend is : ", addressability_result_formatted))
  addressability_result
}

process_agency_agg_bic_addressability <- function(add_mode, agency_name, agency_testing_transactions)
{
  #build composite BIC addressability matrix
  #get bic from testing list 
  bic_contracts <- testing_transactions %>% 
    filter(business_rule_tier == "BIC") %>%
    distinct(contract_name) %>% 
    na.omit() %>% collect() %>% .$contract_name
  
  composite_addressability_matrix <- dplyr_gen_addressability_matrix_df(add_mode, bic_contracts, training_transactions)
  composite_addressability_matrix <- composite_addressability_matrix %>% filter(is.na(product_or_service_code) == FALSE & is.na(naics_code) == FALSE) %>%
    distinct(product_or_service_code, naics_code, addkey)
  
  date_path <- gsub("-", "", Sys.Date())
  dir.create(date_path)
  dir.create(paste0(date_path,"/matrices"))
  file_time_stamp <- gsub(" ", "", Sys.time())
  file_time_stamp <- gsub(":","", file_time_stamp)
  file_contract_name <- gsub("/", "", "bic_composite")
  file_contract_name <- gsub(" ", "_", file_contract_name)
  file_contract_name <- gsub("-", "", file_contract_name)
  write_csv(composite_addressability_matrix, paste0(date_path,"/matrices/",file_contract_name,"_matrix_", file_time_stamp, ".csv"))
  
  result_df <- dplyr_gen_testPhase_df(add_mode, composite_addressability_matrix, agency_testing_transactions, agency_name)
  
  addressability_result_row_count <- result_df %>% count()
  if(addressability_result_row_count >0 )
  {
    addressability_result <- result_df %>% select(dollars_obligated)%>% sum()
  }
  else
  {
    addressability_result <- 0
  }
  
  #actual_obligations <<- opt_get_contract_totals(contract_name)
  addressability_result_formatted <- to_currency(addressability_result, currency_symbol = "$", symbol_first = TRUE, group_size = 3, group_delim = ",", decimal_size = 2,decimal_delim = ".")
  print(paste0( agency_name," addressable spend is : ", addressability_result_formatted))
  result_df
  
}


dplyr_gen_addressability_matrix_df <- function(add_mode, contract_label, training_df)
{
  #builds addressabbility matrix based on 6 factors
  addressability_matrix_df <-  training_df %>% filter(contract_name %in% contract_label) %>%
    arrange( product_or_service_code,naics_code, sbg_flag, women_owned_flag, veteran_owned_flag, minority_owned_business_flag, foreign_government,  co_bus_size_determination_code,  foreign_funding_desc,  firm8a_joint_venture,  dot_certified_disadv_bus,  sdb,  sdb_flag,  hubzone_flag,  sheltered_workshop_flag, srdvob_flag,  other_minority_owned,  baob_flag,  aiob_flag,  naob_flag,  haob_flag,  saaob_flag,  emerging_small_business_flag,  wosb_flag,  edwosb_flag,  jvwosb_flag,  edjvwosb_flag) %>%
    select(product_or_service_code,naics_code, sbg_flag, women_owned_flag, veteran_owned_flag, minority_owned_business_flag, foreign_government,  co_bus_size_determination_code,  foreign_funding_desc,  firm8a_joint_venture,  dot_certified_disadv_bus,  sdb,  sdb_flag,  hubzone_flag,  sheltered_workshop_flag, srdvob_flag,  other_minority_owned,  baob_flag,  aiob_flag,  naob_flag,  haob_flag,  saaob_flag,  emerging_small_business_flag,  wosb_flag,  edwosb_flag,  jvwosb_flag,  edjvwosb_flag) %>%
    collect()
  #adds addressability key to matrix post collection
  addressability_matrix_return <- addressability_matrix_df
  if(add_mode == "ADDR_MRKT"){
  
  addressability_matrix_return <- addressability_matrix_df %>% 
    mutate(addkey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government) )
  }
  
  else if (add_mode == "CASE_PROP")
         {
    addressability_matrix_return <- addressability_matrix_df %>% 
      mutate(addkey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government, "_", co_bus_size_determination_code, "_", foreign_funding_desc, "_", firm8a_joint_venture, "_", dot_certified_disadv_bus, "_", sdb, "_", sdb_flag, "_", hubzone_flag, "_", sheltered_workshop_flag,"_", srdvob_flag, "_", other_minority_owned, "_", baob_flag, "_", aiob_flag, "_", naob_flag, "_", haob_flag, "_", saaob_flag, "_", emerging_small_business_flag, "_", wosb_flag, "_", edwosb_flag, "_", jvwosb_flag, "_", edjvwosb_flag))
         }
        else #mode="PSC_NAICS"
        {
        addressability_matrix_return <- addressability_matrix_df %>% 
        mutate(addkey = paste0(product_or_service_code,"_",naics_code))
        }
  addressability_matrix_return <- addressability_matrix_return %>%distinct()  
  
  addressability_matrix_return
}



dplyr_gen_testPhase_df <- function(add_mode, addressability_matrix, testing_df, contract_name)
{
  date_path <- gsub("-", "", Sys.Date())
  dir.create(date_path)
  file_time_stamp <- gsub(" ", "", Sys.time())
  file_time_stamp <- gsub(":","", file_time_stamp)
  file_contract_name <- gsub("/", "", contract_name)
  file_contract_name <- gsub(" ", "_", file_contract_name)
  file_contract_name <- gsub("-", "", file_contract_name)
  
  
  #prevents summing of duplicate addressability matrix entries across multiple contracts in FAS ops
  addressability_matrix_addkey <- addressability_matrix %>% distinct(addkey) %>% .$addkey
  
  
  if(add_mode == "ADDR_MRKT"){
  addressability_test_result <- testing_df %>% 
                                #filter(level_1_category_group == "GWCM") %>%#
                                filter(addkey %in% addressability_matrix_addkey) %>% collect()
  
  
                              }
  else if(add_mode == "PSC_NAICS")
    #mode="PSC_NAICS"
  {
    addressability_test_result <- testing_df %>% 
      #filter(level_1_category_group == "GWCM") %>%#
      filter(psc_naics_key %in% addressability_matrix_addkey) %>% collect()
  }
  else
  {
    #CASE_PROP
    addressability_test_result <- testing_df %>% 
      #filter(level_1_category_group == "GWCM") %>%#
      filter(case_multikey %in% addressability_matrix_addkey) %>% collect()
  }
  
  
  write_csv(addressability_test_result, paste0(date_path,"/", file_contract_name,"_resultdf_",add_mode,"_",file_time_stamp,".csv"))
  addressability_test_result
}


addressability_injection <- function(addressability_matrix, inject_filename)
{
  #read in addressability rows. 
  signature_update <- read_csv(filename)
  #inject rows in to addressability matrix
  modified_addressability_matrix <- bind_rows(addressabiity_matrix, signature_update)
  #return updated addressability matrix
  modified_addressability_matrix
}


capture_FAS_Training_Awards <- function(regex_pattern, start_date, end_date)
{
    fas_contracts <- raw_df %>% filter(as.Date(date_signed) >= as.Date(start_date) & as.Date(date_signed) <= as.Date(end_date)) %>%
    filter(rlike(reference_piid, regex_pattern) == TRUE) ##%>% 
   ## select(date_signed,reference_piid, idv_ref_idv_piid, managing_agency, contracting_agency_name, product_or_service_code, naics_code, sbg_flag, women_owned_flag, veteran_owned_flag, minority_owned_business_flag, foreign_government, dollars_obligated)
##Do the down select in FAS_dplyr_gen_addressability_matrix_df() based on mode
##There is no loss in speed due to the fact that results have not yet been collected.
    fas_contracts
}


capture_FAS_Sched_Dependent_Training_Awards <- function(regex_pattern, start_date, end_date)
{
     fas_sched_dep <- raw_df %>% filter(as.Date(date_signed) >= as.Date(start_date) & as.Date(date_signed) <= as.Date(end_date)) %>%
       filter(rlike(idv_ref_idv_piid, regex_pattern) == TRUE) ##%>% 
       ##select(date_signed,reference_piid, idv_ref_idv_piid, managing_agency, contracting_agency_name, product_or_service_code, naics_code, sbg_flag, women_owned_flag, veteran_owned_flag, minority_owned_business_flag, foreign_government, dollars_obligated)
##Do the down select in FAS_dplyr_gen_addressability_matrix_df() based on mode
##There is no loss in speed due to the fact that results have not yet been collected.
     fas_sched_dep
}

##Note 1: We need to choose the mode
##Note 2: 
##      2a: We need to pull the direct FAS awards and the 
##      2b: FAS Schedule dependency awards for that mode
##      2c: Remove NA's (PSC or NAICS) from matrix
##Consider Parameterizing the training and test date ranges

FAS_dplyr_gen_addressability_matrix_df <- function(addr_mode, start_date, end_date)
{ ##Add FAS BPA Awards to this matrix
  fas_awards <- capture_FAS_Training_Awards("^GS..[FKQT]", start_date, end_date)
  fas_sched_awards <- capture_FAS_Sched_Dependent_Training_Awards("^GS..[FKQT]", start_date, end_date)
  fas_ref_piids <- fas_awards%>% select(reference_piid) %>% distinct() %>% collect() %>% .$reference_piid
  fas_sched_dep_ref_piids <- fas_sched_awards %>% select(reference_piid) %>% distinct() %>% collect() %>% .$reference_piid
  all_fas_associated_ref_piids <- append(fas_ref_piids, fas_sched_dep_ref_piids)
  
  if(addr_mode == "ADDR_MRKT")
    {addressability_matrix_df <-  raw_df %>% filter(as.Date(date_signed) >= as.Date(start_date) & as.Date(date_signed) <= as.Date(end_date)) %>%
    filter(reference_piid %in% all_fas_associated_ref_piids) %>% 
    distinct( contract_name, product_or_service_code, naics_code, sbg_flag, women_owned_flag, veteran_owned_flag, minority_owned_business_flag, foreign_government) %>%
    arrange( product_or_service_code, naics_code, sbg_flag, women_owned_flag, veteran_owned_flag, minority_owned_business_flag, foreign_government) %>%
    collect()
    #adds addressability key to matrix post collection
    addressability_matrix_return <- addressability_matrix_df %>% 
      mutate(addkey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government) )
 
    }
  else if (addr_mode == "PSC_NAICS")
  {
    addressability_matrix_df <-  raw_df %>% filter(as.Date(date_signed) >= as.Date(start_date) & as.Date(date_signed) <= as.Date(end_date)) %>%
      filter(reference_piid %in% all_fas_associated_ref_piids) %>% 
      distinct( contract_name, product_or_service_code, naics_code) %>%
      arrange( product_or_service_code, naics_code) %>%
      collect()
    #adds addressability key to matrix post collection
    addressability_matrix_return <- addressability_matrix_df %>% 
      mutate(addkey = paste0(product_or_service_code,"_",naics_code) )

  }
  else if(addr_mode == "CASE_PROP")
  {
    addressability_matrix_df <-  raw_df %>% filter(as.Date(date_signed) >= as.Date(start_date) & as.Date(date_signed) <= as.Date(end_date)) %>%
      filter(reference_piid %in% all_fas_associated_ref_piids) %>% 
      distinct( contract_name, product_or_service_code,naics_code, sbg_flag, women_owned_flag, veteran_owned_flag, minority_owned_business_flag, foreign_government,  co_bus_size_determination_code,  foreign_funding_desc,  firm8a_joint_venture,  dot_certified_disadv_bus,  sdb,  sdb_flag,  hubzone_flag,  sheltered_workshop_flag, srdvob_flag,  other_minority_owned,  baob_flag,  aiob_flag,  naob_flag,  haob_flag,  saaob_flag,  emerging_small_business_flag,  wosb_flag,  edwosb_flag,  jvwosb_flag,  edjvwosb_flag) %>%
      arrange( product_or_service_code,naics_code, sbg_flag, women_owned_flag, veteran_owned_flag, minority_owned_business_flag, foreign_government,  co_bus_size_determination_code,  foreign_funding_desc,  firm8a_joint_venture,  dot_certified_disadv_bus,  sdb,  sdb_flag,  hubzone_flag,  sheltered_workshop_flag, srdvob_flag,  other_minority_owned,  baob_flag,  aiob_flag,  naob_flag,  haob_flag,  saaob_flag,  emerging_small_business_flag,  wosb_flag,  edwosb_flag,  jvwosb_flag,  edjvwosb_flag) %>%
      collect() 
    
    addressability_matrix_return <- addressability_matrix_df %>% 
    mutate(addkey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government, "_", co_bus_size_determination_code, "_", foreign_funding_desc, "_", firm8a_joint_venture, "_", dot_certified_disadv_bus, "_", sdb, "_", sdb_flag, "_", hubzone_flag, "_", sheltered_workshop_flag,"_", srdvob_flag, "_", other_minority_owned, "_", baob_flag, "_", aiob_flag, "_", naob_flag, "_", haob_flag, "_", saaob_flag, "_", emerging_small_business_flag, "_", wosb_flag, "_", edwosb_flag, "_", jvwosb_flag, "_", edjvwosb_flag))
   
  }  
  else{print(paste0("Addressability matrix not calculated. addr_mode == ", addr_mode))}
    
  #adds addressability key to matrix post collection
  #addressability_matrix_return <- addressability_matrix_df %>% 
  #  mutate(addkey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government) )
  #addressability_matrix_return
  date_path <- gsub("-", "", Sys.Date())
  dir.create(date_path)
  file_time_stamp <- gsub(" ", "", Sys.time())
  file_time_stamp <- gsub(":","", file_time_stamp)
  #write_csv(addressability_matrix, paste0(date_path,"/",file_contract_name,"_addr_matrix_", bic_or_gsa,"_",add_mode,"_", file_time_stamp, ".csv"))
  addressability_matrix_return <- addressability_matrix_return %>% filter(is.na(product_or_service_code) == FALSE & is.na(naics_code) == FALSE)
  write_csv(addressability_matrix_return, paste0(date_path,"/","FAS_Addressability_Matrix_",addr_mode,"_",file_time_stamp,".csv"))
  addressability_matrix_return
}

opt_get_contract_totals <- function(contract_label)
{
  contract_total_obligations = -1
  contract_total_obligations_count <- testing_transactions %>% 
         filter(contract_name %in% contract_label) %>% 
         select(dollars_obligated) %>% count() %>% collect() %>% .$n
     if(contract_total_obligations_count > 0 )
     {
       contract_total_obligations <- testing_transactions %>% 
         filter(contract_name == contract_label) %>% 
         select(dollars_obligated) %>% collect() %>%  sum()
     }
  contract_total_obligations
}
