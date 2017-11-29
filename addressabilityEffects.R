source("opt_addressability.R")

process_FAS_Addressability <- function(add_mode)
{print("Generating FAS addressability matrix")
  if(add_mode == "ADDR_MRKT"){
                          print(paste0("FAS addressability mode = ", add_mode))
                          tic()
                          fas_base_addressability_matrix <- FAS_dplyr_gen_addressability_matrix_df("^GS..[FKQT]")
                          fas_instruments_addressability_matrix <- FAS_inst_frm_sched_dplyr_gen_addressability_matrix_df("^GS..[FKQT]")
                          toc()
                          }
  else #mode="PSC_NAICS"
                          {
                          print(paste0("FAS addressability mode = ", add_mode)) 
                          tic()
                          fas_base_addressability_matrix <- FAS_PSC_NAICS_dplyr_gen_addressability_matrix_df("^GS..[FKQT]")
                          fas_instruments_addressability_matrix <- FAS_PSC_NAICS_inst_frm_sched_dplyr_gen_addressability_matrix_df("^GS..[FKQT]")
                          toc()
                          }
  print("Removing NA PSC and NAICS observations")
  tic()
  fas_instruments_addressability_matrix <- fas_instruments_addressability_matrix %>%
                                           filter(is.na(product_or_service_code) == FALSE & is.na(naics_code) == FALSE)
  fas_base_addressability_matrix <- fas_base_addressability_matrix %>% 
                                    filter(is.na(product_or_service_code) == FALSE & is.na(naics_code) == FALSE)
  toc()
  
  print("Binding addressable matrices for FAS and FAS schedule derived instruments ")
  tic()
  fas_addressability_matrix <- bind_rows(fas_base_addressability_matrix, fas_instruments_addressability_matrix)
  toc()
  
  write_csv(fas_addressability_matrix, paste0(add_mode,"_FAS_Addressability_Matrix",".csv"))
  #OR 
  #re-write FAS_dplyr_gen_addressability_matrix_df() such that it also performs the FAS sourced BPA function to augment the 
  # fas_addressability_matrix
  print("Generating FY13 addressability")
  tic()
  fy13fas_addressability_df <<- dplyr_gen_testPhase_df(add_mode, fas_addressability_matrix, fy13testing_transactions)
  toc()
  print("Generating FY14 addressability")
  tic()
  fy14fas_addressability_df <<- dplyr_gen_testPhase_df(add_mode, fas_addressability_matrix, fy14testing_transactions)
  toc()
  print("Generating FY15 addressability")
  tic()
  fy15fas_addressability_df <<- dplyr_gen_testPhase_df(add_mode, fas_addressability_matrix, fy15testing_transactions)
  toc()
  print("Generating FY16 addressability")
  tic()
  fy16fas_addressability_df <<- dplyr_gen_testPhase_df(add_mode, fas_addressability_matrix, fy16testing_transactions)
  toc()
  print("Generating FY17 addressability")
  tic()
  fy17fas_addressability_df <<- dplyr_gen_testPhase_df(add_mode, fas_addressability_matrix, fy17testing_transactions)
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
  fas_fy13_bpa_awards <- capture_FAS_Dependent_BPA_Training_Awards("^GS..[FKQT]", "2012-10-01", "2013-09-30")
  toc()
  print("Capturing FY14 external FAS instrument Obligations")
  tic()
  fas_fy14_bpa_awards <- capture_FAS_Dependent_BPA_Training_Awards("^GS..[FKQT]", "2013-10-01", "2014-09-30")
  toc()
  print("Capturing FY15 external FAS instrument Obligations")
  tic()
  fas_fy15_bpa_awards <- capture_FAS_Dependent_BPA_Training_Awards("^GS..[FKQT]", "2014-10-01", "2015-09-30")
  toc()
  print("Capturing FY16 external FAS instrument Obligations")
  tic()
  fas_fy16_bpa_awards <- capture_FAS_Dependent_BPA_Training_Awards("^GS..[FKQT]", "2015-10-01", "2016-09-30")
  toc()
  print("Capturing FY17 external FAS instrument Obligations")
  tic()
  fas_fy17_bpa_awards <- capture_FAS_Dependent_BPA_Training_Awards("^GS..[FKQT]", "2016-10-01", "2017-09-30")
  toc()
  fas_bpa_obligations <- c(fas_fy13_bpa_awards%>%select(dollars_obligated)%>%collect()%>%sum(), fas_fy14_bpa_awards%>%select(dollars_obligated)%>%collect()%>%sum(), fas_fy15_bpa_awards%>%select(dollars_obligated)%>%collect()%>%sum(), fas_fy16_bpa_awards%>%select(dollars_obligated)%>%collect()%>%sum(), fas_fy17_bpa_awards%>%select(dollars_obligated)%>%collect()%>%sum())
  fas_total_obligations <- fas_actual_obligations+fas_bpa_obligations
  FAS_addressability_results <- data_frame(fiscal_year, total_addressability, fas_actual_obligations, fas_bpa_obligations, fas_total_obligations)
  #dir.create(Sys.Date())
  file_time_stamp <- gsub(" ", "", Sys.time())
  file_time_stamp <- gsub(":","", file_time_stamp)
  write_csv(FAS_addressability_results, paste0(add_mode,"_FAS_Addressability_Results",file_time_stamp,".csv"))
  
  
}

process_FAS_contracts <- function()
{
#Loop through each FAS contract
  #build addressability matrix
  #produce addressability by each funding department
  #results should be in the form:
  #[CONTRACT_NAME][FUNDING_DEPARTMENT][OBLIGATIONS][ADDRESSABLE_OBLIGATIONS]
}

process_Agency_BIC_Addressability <- function()
{
 #Loop through each BIC contract
  #build addressability matrix
  #produce addressability by each funding department
  #results should be in the form:
  #[CONTRACT_NAME][FUNDING_DEPARTMENT][OBLIGATIONS][ADDRESSABLE_OBLIGATIONS]  
}

process_bureau_BIC_addressability <- function()
{
#Loop through each BIC contracts 
  #build addressability matrix
  #produce addressability by each funding_agency
  #results should be in the form:
  #[CONTRACT_NAME][FUNDING_DEPARTMENT][FUNDING_AGENCY][OBLIGATIONS][ADDRESSABLE_OBLIGATIONS]  
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
  #STEP 1. Query official_bic_contract column for distinct BICs
  bic_contract_list <- training_transactions %>% 
    distinct(official_bic_contract) %>% na.omit() %>% collect() %>% .$official_bic_contract
  #STEP 2. Query contract_name column for BICs. This has to be done because contract names are different in the 
  # official_bic_contract column from the names used in the contract column
  bic_contracts <<- training_transactions %>% 
    filter(official_bic_contract %in% bic_contract_list) %>% 
    distinct(contract_name) %>%
    collect() %>% 
    .$contract_name
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
