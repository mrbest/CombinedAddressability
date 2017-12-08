source("opt_addressability.R")

process_FAS_Addressability <- function(add_mode, training_start_date, training_end_date)
{
  print("subsetting testing transactions")
  fy13testing_transactions <- raw_df %>% filter(as.Date(date_signed) >= as.Date("2012-10-01") & as.Date(date_signed) <= as.Date("2013-09-30")) 
  fy13testing_transactions <- fy13testing_transactions %>% mutate(addkey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government) )
  fy13testing_transactions <- fy13testing_transactions %>% mutate(psc_naics_key = paste0(product_or_service_code,"_",naics_code))
  fy13testing_transactions <- fy13testing_transactions %>% mutate(case_multikey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government, "_", co_bus_size_determination_code, "_", foreign_funding_desc, "_", firm8a_joint_venture, "_", dot_certified_disadv_bus, "_", sdb, "_", sdb_flag, "_", hubzone_flag, "_", sheltered_workshop_flag,"_", srdvob_flag, "_", other_minority_owned, "_", baob_flag, "_", aiob_flag, "_", naob_flag, "_", haob_flag, "_", saaob_flag, "_", emerging_small_business_flag, "_", wosb_flag, "_", edwosb_flag, "_", jvwosb_flag, "_", edjvwosb_flag))
  
  
  fy14testing_transactions <- raw_df %>% filter(as.Date(date_signed) >= as.Date("2013-10-01") & as.Date(date_signed) <= as.Date("2014-09-30")) 
  fy14testing_transactions <- fy14testing_transactions %>% mutate(addkey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government) )
  fy14testing_transactions <- fy14testing_transactions %>% mutate(psc_naics_key = paste0(product_or_service_code,"_",naics_code))
  fy14testing_transactions <- fy14testing_transactions %>% mutate(case_multikey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government, "_", co_bus_size_determination_code, "_", foreign_funding_desc, "_", firm8a_joint_venture, "_", dot_certified_disadv_bus, "_", sdb, "_", sdb_flag, "_", hubzone_flag, "_", sheltered_workshop_flag,"_", srdvob_flag, "_", other_minority_owned, "_", baob_flag, "_", aiob_flag, "_", naob_flag, "_", haob_flag, "_", saaob_flag, "_", emerging_small_business_flag, "_", wosb_flag, "_", edwosb_flag, "_", jvwosb_flag, "_", edjvwosb_flag))
  
  
  
  fy15testing_transactions <- raw_df %>% filter(as.Date(date_signed) >= as.Date("2014-10-01") & as.Date(date_signed) <= as.Date("2015-09-30")) 
  fy15testing_transactions <- fy15testing_transactions %>% mutate(addkey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government) )
  fy15testing_transactions <- fy15testing_transactions %>% mutate(psc_naics_key = paste0(product_or_service_code,"_",naics_code))
  fy15testing_transactions <- fy15testing_transactions %>% mutate(case_multikey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government, "_", co_bus_size_determination_code, "_", foreign_funding_desc, "_", firm8a_joint_venture, "_", dot_certified_disadv_bus, "_", sdb, "_", sdb_flag, "_", hubzone_flag, "_", sheltered_workshop_flag,"_", srdvob_flag, "_", other_minority_owned, "_", baob_flag, "_", aiob_flag, "_", naob_flag, "_", haob_flag, "_", saaob_flag, "_", emerging_small_business_flag, "_", wosb_flag, "_", edwosb_flag, "_", jvwosb_flag, "_", edjvwosb_flag))
  
  
  fy16testing_transactions <- raw_df %>% filter(as.Date(date_signed) >= as.Date("2015-10-01") & as.Date(date_signed) <= as.Date("2016-09-30")) 
  fy16testing_transactions <- fy16testing_transactions %>% mutate(addkey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government) )
  fy16testing_transactions <- fy16testing_transactions %>% mutate(psc_naics_key = paste0(product_or_service_code,"_",naics_code))
  fy16testing_transactions <- fy16testing_transactions %>% mutate(case_multikey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government, "_", co_bus_size_determination_code, "_", foreign_funding_desc, "_", firm8a_joint_venture, "_", dot_certified_disadv_bus, "_", sdb, "_", sdb_flag, "_", hubzone_flag, "_", sheltered_workshop_flag,"_", srdvob_flag, "_", other_minority_owned, "_", baob_flag, "_", aiob_flag, "_", naob_flag, "_", haob_flag, "_", saaob_flag, "_", emerging_small_business_flag, "_", wosb_flag, "_", edwosb_flag, "_", jvwosb_flag, "_", edjvwosb_flag))

  fy17testing_transactions <- raw_df %>% filter(as.Date(date_signed) >= as.Date("2016-10-01") & as.Date(date_signed) <= as.Date("2017-09-30")) 
  fy17testing_transactions <- fy17testing_transactions %>% mutate(addkey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government) )
  fy17testing_transactions <- fy17testing_transactions %>% mutate(psc_naics_key = paste0(product_or_service_code,"_",naics_code))
  fy17testing_transactions <- fy17testing_transactions %>% mutate(case_multikey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government, "_", co_bus_size_determination_code, "_", foreign_funding_desc, "_", firm8a_joint_venture, "_", dot_certified_disadv_bus, "_", sdb, "_", sdb_flag, "_", hubzone_flag, "_", sheltered_workshop_flag,"_", srdvob_flag, "_", other_minority_owned, "_", baob_flag, "_", aiob_flag, "_", naob_flag, "_", haob_flag, "_", saaob_flag, "_", emerging_small_business_flag, "_", wosb_flag, "_", edwosb_flag, "_", jvwosb_flag, "_", edjvwosb_flag))  

  print("Generating FAS addressability matrix")
#  if(add_mode == "ADDR_MRKT"){
#                          print(paste0("FAS addressability mode = ", add_mode))
#                          tic()
#                          fas_base_addressability_matrix <- FAS_dplyr_gen_addressability_matrix_df(add_mode)
#                          #fas_instruments_addressability_matrix <- FAS_inst_frm_sched_dplyr_gen_addressability_matrix_df("^GS..[FKQT]")
#                          toc()
#                          }
#  else if (add_mode == "PSC_NAICS")
#                          {
#                          print(paste0("FAS addressability mode = ", add_mode)) 
#                          tic()
#                          fas_base_addressability_matrix <- FAS_dplyr_gen_addressability_matrix_df(add_mode)
#                          fas_instruments_addressability_matrix <- FAS_PSC_NAICS_inst_frm_sched_dplyr_gen_addressability_matrix_df("^GS..[FKQT]")
#                          toc()
#                          }
#  else #add_mode == "CASE_PROP"
#                          {
#                            print(paste0("FAS addressability mode = ", add_mode)) 
#                            tic()
#                            fas_base_addressability_matrix <- FAS_PSC_NAICS_dplyr_gen_addressability_matrix_df("^GS..[FKQT]")
#                            fas_instruments_addressability_matrix <- FAS_PSC_NAICS_inst_frm_sched_dplyr_gen_addressability_matrix_df("^GS..[FKQT]")
#                            toc()
#                           }
  print(paste0("FAS addressability matrix production mode = ", add_mode))
  tic()
  fas_addressability_matrix <- FAS_dplyr_gen_addressability_matrix_df(add_mode, training_start_date, training_end_date)
  toc()
  
  
  #print("Removing NA PSC and NAICS observations")
  #tic()
  #fas_instruments_addressability_matrix <- fas_instruments_addressability_matrix %>%
  #                                         filter(is.na(product_or_service_code) == FALSE & is.na(naics_code) == FALSE)
  #fas_base_addressability_matrix <- fas_base_addressability_matrix %>% 
  #                                  filter(is.na(product_or_service_code) == FALSE & is.na(naics_code) == FALSE)
  #toc()
  
  #print("Binding addressable matrices for FAS and FAS schedule derived instruments ")
  #tic()
  #fas_addressability_matrix <- bind_rows(fas_base_addressability_matrix, fas_instruments_addressability_matrix)
  #toc()
  
  
  #OR 
  #re-write FAS_dplyr_gen_addressability_matrix_df() such that it also performs the FAS sourced BPA function to augment the 
  # fas_addressability_matrix
  print("Generating FY13 addressability")
  tic()
  fy13fas_addressability_df <<- dplyr_gen_testPhase_df(add_mode, fas_addressability_matrix, fy13testing_transactions, "FAS_FY13")
  toc()
  print("Generating FY14 addressability")
  tic()
  fy14fas_addressability_df <<- dplyr_gen_testPhase_df(add_mode, fas_addressability_matrix, fy14testing_transactions, "FAS_FY14")
  toc()
  print("Generating FY15 addressability")
  tic()
  fy15fas_addressability_df <<- dplyr_gen_testPhase_df(add_mode, fas_addressability_matrix, fy15testing_transactions, "FAS_FY15")
  toc()
  print("Generating FY16 addressability")
  tic()
  fy16fas_addressability_df <<- dplyr_gen_testPhase_df(add_mode, fas_addressability_matrix, fy16testing_transactions, "FAS_FY16")
  toc()
  print("Generating FY17 addressability")
  tic()
  fy17fas_addressability_df <<- dplyr_gen_testPhase_df(add_mode, fas_addressability_matrix, fy17testing_transactions, "FAS_FY17")
  toc()
  
  total_addressability <- double()
  fiscal_year <- numeric()
  total_addressability <- c(fy13fas_addressability_df %>% select(dollars_obligated) %>% sum(), 
                            fy14fas_addressability_df %>% select(dollars_obligated) %>% sum(),
                            fy15fas_addressability_df %>% select(dollars_obligated) %>% sum(),
                            fy16fas_addressability_df %>% select(dollars_obligated) %>% sum(),
                            fy17fas_addressability_df %>% select(dollars_obligated) %>% sum())
  
  fiscal_year <- c(2013, 2014, 2015, 2016, 2017)
  
  print("Capturing FY13 FAS Obligations")
  tic()
  fas_fy13_awards <- capture_FAS_Training_Awards("^GS..[FKQT]", "2012-10-01", "2013-09-30")
  toc()
  print("Capturing FY14 FAS Obligations")
  tic()
  fas_fy14_awards <- capture_FAS_Training_Awards("^GS..[FKQT]", "2013-10-01", "2014-09-30")
  toc()
  print("Capturing FY15 FAS Obligations")
  tic()
  fas_fy15_awards <- capture_FAS_Training_Awards("^GS..[FKQT]", "2014-10-01", "2015-09-30")
  toc()
  print("Capturing FY16 FAS Obligations")
  tic()
  fas_fy16_awards <- capture_FAS_Training_Awards("^GS..[FKQT]", "2015-10-01", "2016-09-30")
  toc()
  print("Capturing FY17 FAS Obligations")
  tic()
  fas_fy17_awards <- capture_FAS_Training_Awards("^GS..[FKQT]", "2016-10-01", "2017-09-30")
  toc()
  fas_actual_obligations <- c(fas_fy13_awards%>%select(dollars_obligated)%>%collect()%>%sum(), fas_fy14_awards%>%select(dollars_obligated)%>%collect()%>%sum(), fas_fy15_awards%>%select(dollars_obligated)%>%collect()%>%sum(), fas_fy16_awards%>%select(dollars_obligated)%>%collect()%>%sum(), fas_fy17_awards%>%select(dollars_obligated)%>%collect()%>%sum())
  
  print("Capturing FY13 external FAS instrument Obligations")
  tic()
  fas_fy13_bpa_awards <- capture_FAS_Sched_Dependent_Training_Awards("^GS..[FKQT]", "2012-10-01", "2013-09-30")
  toc()
  print("Capturing FY14 external FAS instrument Obligations")
  tic()
  fas_fy14_bpa_awards <- capture_FAS_Sched_Dependent_Training_Awards("^GS..[FKQT]", "2013-10-01", "2014-09-30")
  toc()
  print("Capturing FY15 external FAS instrument Obligations")
  tic()
  fas_fy15_bpa_awards <- capture_FAS_Sched_Dependent_Training_Awards("^GS..[FKQT]", "2014-10-01", "2015-09-30")
  toc()
  print("Capturing FY16 external FAS instrument Obligations")
  tic()
  fas_fy16_bpa_awards <- capture_FAS_Sched_Dependent_Training_Awards("^GS..[FKQT]", "2015-10-01", "2016-09-30")
  toc()
  print("Capturing FY17 external FAS instrument Obligations")
  tic()
  fas_fy17_bpa_awards <- capture_FAS_Sched_Dependent_Training_Awards("^GS..[FKQT]", "2016-10-01", "2017-09-30")
  toc()
  fas_bpa_obligations <- c(fas_fy13_bpa_awards%>%select(dollars_obligated)%>%collect()%>%sum(), fas_fy14_bpa_awards%>%select(dollars_obligated)%>%collect()%>%sum(), fas_fy15_bpa_awards%>%select(dollars_obligated)%>%collect()%>%sum(), fas_fy16_bpa_awards%>%select(dollars_obligated)%>%collect()%>%sum(), fas_fy17_bpa_awards%>%select(dollars_obligated)%>%collect()%>%sum())
  fas_total_obligations <- fas_actual_obligations+fas_bpa_obligations
  FAS_addressability_results <- data_frame(fiscal_year, total_addressability, fas_actual_obligations, fas_bpa_obligations, fas_total_obligations)
  #dir.create(Sys.Date())
  file_time_stamp <- gsub(" ", "", Sys.time())
  file_time_stamp <- gsub(":","", file_time_stamp)
  write_csv(FAS_addressability_results, paste0(add_mode,"_FAS_Addressability_Results",file_time_stamp,".csv"))
  
  
}


agency_bic_addressable_obs <- function(bic, test_period_start, test_period_end)
{
#build addressability matrix  
}


process_gsa_contracts <- function(add_mode)
{ #declare vector accumulators for contract name, addressable obligations result and contract actual obligations 
  actual_obligations_vector <- double()
  addressable_market_vector <- double()
  contract_name_vector <- character()
  #get dummy addressable matric for master addressable matrix creation
  master_addressability_matrix <- dplyr_gen_addressability_matrix_df(add_mode, "OS3", training_transactions)
  #count the rows and make negative in prep for deletion
  rowcount <- master_addressability_matrix %>% count() %>% .$n 
  rowcount <- rowcount * -1
  #set master_addressability_matrix up for recieving addressability matrices
  master_addressability_matrix <<- master_addressability_matrix[-1:rowcount, ]
  
  #declare and query list of GSA contracts
  gsa_contracts <- training_transactions %>% 
    filter(managing_agency == "GSA") %>% 
    distinct(contract_name) %>% 
    collect() %>% .$contract_name
  gsa_contracts <- na.omit(gsa_contracts)
  #set up sentinel for looping through GSA contracts  
  contract_count <- length(gsa_contracts)
  for(i in 1:contract_count)
  {##accumulate addressable market and actual obligations contract by contract
    addressable_market_vector <- append(addressable_market_vector, process_one_contract("_GSA_",add_mode, gsa_contracts[i]))
    contract_name_vector <- append(contract_name_vector, gsa_contracts[i])
    actual_obligations_vector <- append(actual_obligations_vector, opt_get_contract_totals(gsa_contracts[i]))
  }
  #write result for all contracts to a dataframe  
  gsa_result_df <- data_frame(contract_name_vector, actual_obligations_vector, addressable_market_vector)
  gsa_result_df
}


process_bic_contracts <- function(add_mode)
{
  #declare vector accumulators for contract name, addressable obligations result and contract actual obligations 
  actual_obligations_vector <- double()
  addressable_market_vector <- double()
  contract_name_vector <- character()
  #get dummy addressable matric for master addressable matrix creation
  master_addressability_matrix <- dplyr_gen_addressability_matrix_df(add_mode, "OS3", training_transactions)
  #count the rows and make negative in prep for deletion
  rowcount <- master_addressability_matrix %>% count() %>% .$n 
  rowcount <- rowcount * -1
  #set master_addressability_matrix up for recieving addressability matrices
  master_addressability_matrix <<- master_addressability_matrix[-1:rowcount, ]
  #declare and query list of BIC contracts
  #STEP 1. Query for distinct BICs
  bic_contracts <- training_transactions %>% 
          filter(business_rule_tier == "BIC") %>%
          distinct(contract_name) %>% 
          na.omit() %>% collect() %>% .$contract_name
  #STEP 2. Query contract_name column for BICs. This has to be done because contract names are different in the 
  # official_bic_contract column from the names used in the contract column
  #bic_contracts <<- training_transactions %>% 
  #  filter(official_bic_contract %in% bic_contract_list) %>% 
  #  distinct(contract_name) %>%
  #  collect() %>% 
  #  .$contract_name
  #set up sentinel for looping through GSA contracts 
  contract_count <- length(bic_contracts)
  for(i in 1:contract_count)
  { ##accumulate addressable market and actual obligations contract by contract
    addressable_market_vector <- append(addressable_market_vector, process_one_contract("_BIC_",add_mode, bic_contracts[i]))
    contract_name_vector <- append(contract_name_vector, bic_contracts[i])
    actual_obligations_vector <- append(actual_obligations_vector, opt_get_contract_totals(bic_contracts[i]))
  }
  
  bic_result_df <- data_frame(contract_name_vector, actual_obligations_vector, addressable_market_vector)
  bic_result_df
}
