refined_df <- raw_df %>% select(award_type_description, vendor_name, co_bus_size_determination_code, contracting_agency_name, 
                                contracting_office_name, current_completion_date, date_signed, last_modified_date, department_name,
                                eight_a_flag, funding_agency_name, funding_department_name, funding_office_name, minority_owned_business_flag,   
                                naics_code, official_bic_contract, bic_pipeline_contract, bic_status, piid, product_or_service_code, 
                                reference_piid, small_business_flag, type_of_set_aside, type_of_set_aside_description, ultimate_completion_date, 
                                level_1_category, level_2_category, level_3_category, level_1_category_group, vendor_duns_number,veteran_owned_flag,
                                women_owned_flag, base_current_contract_value, base_dollars_obligated, base_ultimate_contract_value, dollars_obligated,              
                                sbg_flag, sheltered_workshop_flag, foreign_government, modification_number, reference_modification_number, funding_dod_or_civilian,
                                contracting_dod_or_civilian, schedule_identifier, contract_name, managing_agency, agency_designated_tier, definitive_contract,
                                bic_pipeline_flag, schedule_number, schedule_title, business_rule_tier)         


mini_df <- raw_df %>% select(co_bus_size_determination_code, date_signed, eight_a_flag, minority_owned_business_flag,   
                                naics_code, official_bic_contract, bic_pipeline_contract, bic_status, piid, product_or_service_code, 
                                reference_piid, small_business_flag, type_of_set_aside, type_of_set_aside_description, veteran_owned_flag,
                                women_owned_flag, base_current_contract_value, base_dollars_obligated, base_ultimate_contract_value, dollars_obligated,              
                                sbg_flag, foreign_government, modification_number, reference_modification_number, funding_dod_or_civilian,
                                contracting_dod_or_civilian, schedule_identifier, contract_name, managing_agency, agency_designated_tier, definitive_contract,
                                bic_pipeline_flag, schedule_number, schedule_title, level_1_category, level_2_category)