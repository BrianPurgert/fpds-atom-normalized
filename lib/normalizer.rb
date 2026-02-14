module Normalizer
  module_function

  # Determine record type from content node
  def record_type(content_node)
    name = content_node.name.downcase
    case name
    when 'award' then 'award'
    when 'idv' then 'IDV'
    when 'othertransactionaward' then 'OtherTransactionAward'
    when 'othertransactionidv' then 'OtherTransactionIDV'
    else name
    end
  end

  # Safe text extraction
  def xt(node, xpath)
    node.at_xpath(xpath)&.text&.strip
  end

  # Safe attribute extraction
  def xa(node, xpath, attr = 'description')
    node.at_xpath(xpath)&.[](attr)&.strip
  end

  # Extract competition fields
  def extract_competition(cn)
    {
      extent_competed:                        xt(cn, './/competition/extentCompeted'),
      solicitation_procedures:                xt(cn, './/competition/solicitationProcedures'),
      type_of_set_aside:                      xt(cn, './/competition/typeOfSetAside'),
      type_of_set_aside_source:               xt(cn, './/competition/typeOfSetAsideSource'),
      evaluated_preference:                   xt(cn, './/competition/evaluatedPreference'),
      number_of_offers_received:              xt(cn, './/competition/numberOfOffersReceived')&.to_i,
      number_of_offers_source:                xt(cn, './/competition/numberOfOffersSource'),
      commercial_item_acquisition_procedures: xt(cn, './/competition/commercialItemAcquisitionProcedures'),
      commercial_item_test_program:           xt(cn, './/competition/commercialItemTestProgram'),
      a76_action:                             xt(cn, './/competition/A76Action'),
      fed_biz_opps:                           xt(cn, './/competition/fedBizOpps'),
      local_area_set_aside:                   xt(cn, './/competition/localAreaSetAside'),
      fair_opportunity_limited_sources:       xt(cn, './/competition/statutoryExceptionToFairOpportunity'),
      reason_not_competed:                    xt(cn, './/competition/reasonNotCompeted'),
      competitive_procedures:                 xt(cn, './/competition/competitiveProcedures'),
      research:                               xt(cn, './/competition/research'),
      small_business_competitiveness_demo:    xt(cn, './/competition/smallBusinessCompetitivenessDemonstrationProgram'),
      idv_type_of_set_aside:                  xt(cn, './/competition/IDVTypeOfSetAside'),
      idv_number_of_offers_received:          xt(cn, './/competition/IDVNumberOfOffersReceived')&.to_i,
    }.compact
  end

  # Extract contract data fields
  def extract_contract_data(cn)
    {
      cost_or_pricing_data:                            xt(cn, './/contractData/costOrPricingData'),
      contract_financing:                              xt(cn, './/contractData/contractFinancing'),
      gfe_gfp:                                         xt(cn, './/contractData/GFE_GFP'),
      sea_transportation:                              xt(cn, './/contractData/seaTransportation'),
      undefinitized_action:                             xt(cn, './/contractData/undefinitizedAction'),
      consolidated_contract:                           xt(cn, './/contractData/consolidatedContract'),
      performance_based_service_contract:              xt(cn, './/contractData/performanceBasedServiceContract'),
      multi_year_contract:                             xt(cn, './/contractData/multiYearContract'),
      contingency_humanitarian_peacekeeping_operation: xt(cn, './/contractData/contingencyHumanitarianPeacekeepingOperation'),
      purchase_card_as_payment_method:                 xt(cn, './/contractData/purchaseCardAsPaymentMethod'),
      number_of_actions:                               xt(cn, './/contractData/numberOfActions'),
      referenced_idv_type:                             xt(cn, './/contractData/referencedIDVType'),
      referenced_idv_multiple_or_single:               xt(cn, './/contractData/referencedIDVMultipleOrSingle'),
      major_program_code:                              xt(cn, './/contractData/majorProgramCode'),
      national_interest_action_code:                   xt(cn, './/contractData/nationalInterestActionCode'),
      cost_accounting_standards_clause:                xt(cn, './/contractData/costAccountingStandardsClause'),
      inherently_governmental_function:                xt(cn, './/contractData/inherentlyGovernmentalFunction'),
      solicitation_id:                                 xt(cn, './/contractData/solicitationID'),
      type_of_idc:                                     xt(cn, './/contractData/typeOfIDC'),
      multiple_or_single_award_idc:                    xt(cn, './/contractData/multipleOrSingleAwardIDC'),
    }.compact
  end

  # Extract dollar values
  def extract_dollar_values(cn, parsers)
    {
      base_and_exercised_options_value:       parsers.parse_float(xt(cn, './/dollarValues/baseAndExercisedOptionsValue')),
      total_obligated_amount:                 parsers.parse_float(xt(cn, './/totalDollarValues/totalObligatedAmount')),
      total_base_and_all_options_value:       parsers.parse_float(xt(cn, './/totalDollarValues/totalBaseAndAllOptionsValue')),
      total_base_and_exercised_options_value: parsers.parse_float(xt(cn, './/totalDollarValues/totalBaseAndExercisedOptionsValue')),
    }.compact
  end

  # Extract legislative mandates
  def extract_legislative_mandates(cn)
    {
      clinger_cohen_act:                    xt(cn, './/legislativeMandates/ClingerCohenAct'),
      construction_wage_rate_requirements:  xt(cn, './/legislativeMandates/constructionWageRateRequirements'),
      labor_standards:                      xt(cn, './/legislativeMandates/laborStandards'),
      materials_supplies_articles_equipment: xt(cn, './/legislativeMandates/materialsSuppliesArticlesEquipment'),
      interagency_contracting_authority:    xt(cn, './/legislativeMandates/interagencyContractingAuthority'),
      other_statutory_authority:            xt(cn, './/legislativeMandates/otherStatutoryAuthority'),
    }.compact
  end

  # Extract place of performance
  def extract_place_of_performance(cn)
    {
      pop_street_address:        xt(cn, './/placeOfPerformance/principalPlaceOfPerformance/streetAddress'),
      pop_city:                  xt(cn, './/placeOfPerformance/principalPlaceOfPerformance/city'),
      pop_state_code:            xt(cn, './/placeOfPerformance/principalPlaceOfPerformance/stateCode'),
      pop_zip_code:              xt(cn, './/placeOfPerformance/placeOfPerformanceZIPCode'),
      pop_country_code:          xt(cn, './/placeOfPerformance/principalPlaceOfPerformance/countryCode'),
      pop_congressional_district: xt(cn, './/placeOfPerformance/placeOfPerformanceCongressionalDistrict'),
    }.compact
  end

  # Extract dates
  def extract_dates(cn, parsers)
    {
      signed_date:             parsers.parse_date(xt(cn, './/relevantContractDates/signedDate')),
      current_completion_date: parsers.parse_date(xt(cn, './/relevantContractDates/currentCompletionDate')),
      ultimate_completion_date: parsers.parse_date(xt(cn, './/relevantContractDates/ultimateCompletionDate')),
    }.compact
  end

  # Extract transaction info
  def extract_transaction_info(cn, parsers)
    {
      created_by:         xt(cn, './/transactionInformation/createdBy'),
      created_date:       parsers.parse_datetime(xt(cn, './/transactionInformation/createdDate')),
      last_modified_by:   xt(cn, './/transactionInformation/lastModifiedBy'),
      transaction_status: xt(cn, './/transactionInformation/status'),
      approved_by:        xt(cn, './/transactionInformation/approvedBy'),
      approved_date:      parsers.parse_datetime(xt(cn, './/transactionInformation/approvedDate')),
      closed_status:      xt(cn, './/transactionInformation/closedStatus'),
      closed_by:          xt(cn, './/transactionInformation/closedBy'),
      closed_date:        parsers.parse_datetime(xt(cn, './/transactionInformation/closedDate')),
    }.compact
  end

  # Extract contract marketing data
  def extract_contract_marketing(cn)
    {
      fee_paid_for_use_of_service:  xt(cn, './/contractMarketingData/feePaidForUseOfService'),
      who_can_use:                  xt(cn, './/contractMarketingData/whoCanUse'),
      ordering_procedure:           xt(cn, './/contractMarketingData/orderingProcedure'),
      individual_order_limit:       xt(cn, './/contractMarketingData/individualOrderLimit'),
      type_of_fee_for_use_of_service: xt(cn, './/contractMarketingData/typeOfFeeForUseOfService'),
      contract_marketing_email:     xt(cn, './/contractMarketingData/emailAddress'),
    }.compact
  end

  # Extract product/service info (beyond FK fields)
  def extract_product_service_info(cn)
    {
      claimant_program_code:                          xt(cn, './/productOrServiceInformation/claimantProgramCode'),
      contract_bundling:                              xt(cn, './/productOrServiceInformation/contractBundling'),
      country_of_origin:                              xt(cn, './/productOrServiceInformation/countryOfOrigin'),
      information_technology_commercial_item_category: xt(cn, './/productOrServiceInformation/informationTechnologyCommercialItemCategory'),
      manufacturing_organization_type:                xt(cn, './/productOrServiceInformation/manufacturingOrganizationType'),
      place_of_manufacture:                           xt(cn, './/productOrServiceInformation/placeOfManufacture'),
      recovered_material_clauses:                     xt(cn, './/productOrServiceInformation/recoveredMaterialClauses'),
      system_equipment_code:                          xt(cn, './/productOrServiceInformation/systemEquipmentCode'),
      use_of_epa_designated_products:                 xt(cn, './/productOrServiceInformation/useOfEPADesignatedProducts'),
    }.compact
  end

  # Extract miscellaneous fields
  def extract_misc(cn)
    {
      foreign_funding:                                xt(cn, './/purchaserInformation/foreignFunding'),
      contracting_officer_business_size_determination: xt(cn, './/vendor/contractingOfficerBusinessSizeDetermination'),
      subcontract_plan:                               xt(cn, './/preferencePrograms/subcontractPlan'),
    }.compact
  end

  # Extract award/contract ID fields
  def extract_id_fields(cn)
    {
      referenced_idv_piid:       xt(cn, './/awardID/referencedIDVID/PIID') || xt(cn, './/contractID/referencedIDVID/PIID'),
      referenced_idv_mod_number: xt(cn, './/awardID/referencedIDVID/modNumber') || xt(cn, './/contractID/referencedIDVID/modNumber'),
      referenced_idv_agency_id:  xt(cn, './/awardID/referencedIDVID/agencyID') || xt(cn, './/contractID/referencedIDVID/agencyID'),
      transaction_number:        xt(cn, './/awardID/awardContractID/transactionNumber'),
    }.compact
  end

  # Collect all normalized fields for contract_actions
  def extract_all_action_fields(cn, parsers)
    fields = { record_type: record_type(cn) }
    fields.merge!(extract_id_fields(cn))
    fields.merge!(extract_competition(cn))
    fields.merge!(extract_contract_data(cn))
    fields.merge!(extract_dollar_values(cn, parsers))
    fields.merge!(extract_legislative_mandates(cn))
    fields.merge!(extract_place_of_performance(cn))
    fields.merge!(extract_dates(cn, parsers))
    fields.merge!(extract_transaction_info(cn, parsers))
    fields.merge!(extract_contract_marketing(cn))
    fields.merge!(extract_product_service_info(cn))
    fields.merge!(extract_misc(cn))
    fields
  end

  # Helper to safely convert to boolean
  def to_bool(val)
    return nil if val.nil?
    return val if val == true || val == false
    v = val.to_s.downcase.strip
    return true if %w[true t yes y 1].include?(v)
    return false if %w[false f no n 0].include?(v)
    nil
  end

  # Extract vendor details for fpds_contract_vendor_details
  def extract_vendor_details(cn, parsers)
    v = cn.at_xpath('.//vendor')
    return nil unless v

    sd = v.at_xpath('.//vendorSiteDetails')
    return nil unless sd

    h = {}
    # Header
    hdr = v.at_xpath('.//vendorHeader')
    if hdr
      h[:vendor_name]                     = xt(hdr, './vendorName')
      h[:vendor_alternate_name]           = xt(hdr, './vendorAlternateName')
      h[:vendor_legal_organization_name]  = xt(hdr, './vendorLegalOrganizationName')
      h[:vendor_doing_business_as_name]   = xt(hdr, './vendorDoingBusinessAsName')
      h[:vendor_enabled]                  = to_bool(xt(hdr, './vendorEnabled'))
    end

    # Entity Identifiers
    h[:uei]                      = xt(sd, './/vendorUEIInformation/UEI')
    h[:ultimate_parent_uei]      = xt(sd, './/vendorUEIInformation/ultimateParentUEI')
    h[:uei_legal_business_name]  = xt(sd, './/vendorUEIInformation/UEILegalBusinessName')
    h[:ultimate_parent_uei_name] = xt(sd, './/vendorUEIInformation/ultimateParentUEIName')
    h[:cage_code]                = xt(sd, './/entityIdentifiers/cageCode')

    # Location
    loc = sd.at_xpath('.//vendorLocation')
    if loc
      h[:street_address]   = xt(loc, './streetAddress')
      h[:city]             = xt(loc, './city')
      h[:state]            = xt(loc, './state')
      h[:zip_code]         = xt(loc, './ZIPCode')
      h[:country_code]     = xt(loc, './countryCode')
      h[:phone_no]         = xt(loc, './phoneNo')
      h[:fax_no]           = xt(loc, './faxNo')
      h[:congressional_district]         = xt(loc, './congressionalDistrictCode')
      h[:vendor_location_disabled_flag]  = to_bool(xt(loc, './vendorLocationDisabledFlag'))
      h[:entity_data_source]             = xt(loc, './entityDataSource')
    end

    # CCR Registration
    h[:registration_date] = parsers.parse_date(xt(sd, './/ccrRegistrationDetails/registrationDate'))
    h[:renewal_date]      = parsers.parse_date(xt(sd, './/ccrRegistrationDetails/renewalDate'))

    h[:vendor_alternate_site_code] = xt(sd, './vendorAlternateSiteCode')

    # Socio-Economic Indicators
    sei = sd.at_xpath('.//vendorSocioEconomicIndicators')
    if sei
      h[:is_alaskan_native_owned_corporation_or_firm]                          = to_bool(xt(sei, './isAlaskanNativeOwnedCorporationOrFirm'))
      h[:is_american_indian_owned]                                             = to_bool(xt(sei, './isAmericanIndianOwned'))
      h[:is_indian_tribe]                                                      = to_bool(xt(sei, './isIndianTribe'))
      h[:is_native_hawaiian_owned_organization_or_firm]                        = to_bool(xt(sei, './isNativeHawaiianOwnedOrganizationOrFirm'))
      h[:is_tribally_owned_firm]                                               = to_bool(xt(sei, './isTriballyOwnedFirm'))
      h[:is_veteran_owned]                                                     = to_bool(xt(sei, './isVeteranOwned'))
      h[:is_service_related_disabled_veteran_owned_business]                   = to_bool(xt(sei, './isServiceRelatedDisabledVeteranOwnedBusiness'))
      h[:is_women_owned]                                                       = to_bool(xt(sei, './isWomenOwned'))
      h[:is_women_owned_small_business]                                        = to_bool(xt(sei, './isWomenOwnedSmallBusiness'))
      h[:is_economically_disadvantaged_women_owned_small_business]             = to_bool(xt(sei, './isEconomicallyDisadvantagedWomenOwnedSmallBusiness'))
      h[:is_joint_venture_women_owned_small_business]                          = to_bool(xt(sei, './isJointVentureWomenOwnedSmallBusiness'))
      h[:is_joint_venture_economically_disadvantaged_women_owned_small_business] = to_bool(xt(sei, './isJointVentureEconomicallyDisadvantagedWomenOwnedSmallBusiness'))
      h[:is_small_business]                                                    = to_bool(xt(sei, './isSmallBusiness'))
      h[:is_very_small_business]                                               = to_bool(xt(sei, './isVerySmallBusiness'))
      # Minority Owned
      mo = sei.at_xpath('./minorityOwned')
      if mo
        h[:is_minority_owned]                                   = to_bool(xt(mo, './isMinorityOwned'))
        h[:is_subcontinent_asian_american_owned_business]        = to_bool(xt(mo, './isSubContinentAsianAmericanOwnedBusiness'))
        h[:is_asian_pacific_american_owned_business]             = to_bool(xt(mo, './isAsianPacificAmericanOwnedBusiness'))
        h[:is_black_american_owned_business]                     = to_bool(xt(mo, './isBlackAmericanOwnedBusiness'))
        h[:is_hispanic_american_owned_business]                  = to_bool(xt(mo, './isHispanicAmericanOwnedBusiness'))
        h[:is_native_american_owned_business]                    = to_bool(xt(mo, './isNativeAmericanOwnedBusiness'))
        h[:is_other_minority_owned]                              = to_bool(xt(mo, './isOtherMinorityOwned'))
      end
    end

    # Business Types
    bt = sd.at_xpath('.//vendorBusinessTypes')
    if bt
      h[:is_community_developed_corporation_owned_firm] = to_bool(xt(bt, './isCommunityDevelopedCorporationOwnedFirm'))
      h[:is_labor_surplus_area_firm]                    = to_bool(xt(bt, './isLaborSurplusAreaFirm'))
      fg = bt.at_xpath('./federalGovernment')
      if fg
        h[:is_federal_government]                              = to_bool(xt(fg, './isFederalGovernment'))
        h[:is_federally_funded_research_and_development_corp]  = to_bool(xt(fg, './isFederallyFundedResearchAndDevelopmentCorp'))
        h[:is_federal_government_agency]                       = to_bool(xt(fg, './isFederalGovernmentAgency'))
      end
      h[:is_state_government] = to_bool(xt(bt, './isStateGovernment'))
      lg = bt.at_xpath('./localGovernment')
      if lg
        h[:is_local_government]                    = to_bool(xt(lg, './isLocalGovernment'))
        h[:is_city_local_government]               = to_bool(xt(lg, './isCityLocalGovernment'))
        h[:is_county_local_government]             = to_bool(xt(lg, './isCountyLocalGovernment'))
        h[:is_inter_municipal_local_government]    = to_bool(xt(lg, './isInterMunicipalLocalGovernment'))
        h[:is_local_government_owned]              = to_bool(xt(lg, './isLocalGovernmentOwned'))
        h[:is_municipality_local_government]       = to_bool(xt(lg, './isMunicipalityLocalGovernment'))
        h[:is_school_district_local_government]    = to_bool(xt(lg, './isSchoolDistrictLocalGovernment'))
        h[:is_township_local_government]           = to_bool(xt(lg, './isTownshipLocalGovernment'))
      end
      h[:is_tribal_government]  = to_bool(xt(bt, './isTribalGovernment'))
      h[:is_foreign_government] = to_bool(xt(bt, './isForeignGovernment'))
      bo = bt.at_xpath('./businessOrOrganizationType')
      if bo
        h[:is_corporate_entity_not_tax_exempt]                  = to_bool(xt(bo, './isCorporateEntityNotTaxExempt'))
        h[:is_corporate_entity_tax_exempt]                      = to_bool(xt(bo, './isCorporateEntityTaxExempt'))
        h[:is_partnership_or_limited_liability_partnership]     = to_bool(xt(bo, './isPartnershipOrLimitedLiabilityPartnership'))
        h[:is_sole_proprietorship]                              = to_bool(xt(bo, './isSolePropreitorship'))
        h[:is_small_agricultural_cooperative]                   = to_bool(xt(bo, './isSmallAgriculturalCooperative'))
        h[:is_international_organization]                       = to_bool(xt(bo, './isInternationalOrganization'))
        h[:is_us_government_entity]                             = to_bool(xt(bo, './isUSGovernmentEntity'))
      end
    end

    # Certifications
    vc = sd.at_xpath('.//vendorCertifications')
    if vc
      h[:is_dot_certified_disadvantaged_business_enterprise]  = to_bool(xt(vc, './isDOTCertifiedDisadvantagedBusinessEnterprise'))
      h[:is_self_certified_small_disadvantaged_business]      = to_bool(xt(vc, './isSelfCertifiedSmallDisadvantagedBusiness'))
      h[:is_sba_certified_small_disadvantaged_business]       = to_bool(xt(vc, './isSBACertifiedSmallDisadvantagedBusiness'))
      h[:is_sba_certified_8a_program_participant]             = to_bool(xt(vc, './isSBACertified8AProgramParticipant'))
      h[:is_self_certified_hubzone_joint_venture]             = to_bool(xt(vc, './isSelfCertifiedHUBZoneJointVenture'))
      h[:is_sba_certified_hubzone]                            = to_bool(xt(vc, './isSBACertifiedHUBZone'))
      h[:is_sba_certified_8a_joint_venture]                   = to_bool(xt(vc, './isSBACertified8AJointVenture'))
    end

    # Organization Factors
    of = sd.at_xpath('.//vendorOrganizationFactors')
    if of
      h[:organizational_type]             = xt(of, './organizationalType')
      h[:is_sheltered_workshop]           = to_bool(xt(of, './isShelteredWorkshop'))
      h[:is_limited_liability_corporation] = to_bool(xt(of, './isLimitedLiabilityCorporation'))
      h[:is_subchapter_s_corporation]     = to_bool(xt(of, './isSubchapterSCorporation'))
      h[:is_foreign_owned_and_located]    = to_bool(xt(of, './isForeignOwnedAndLocated'))
      h[:country_of_incorporation]        = xt(of, './countryOfIncorporation')
      h[:state_of_incorporation]          = xt(of, './stateOfIncorporation')
      ps = of.at_xpath('./profitStructure')
      if ps
        h[:is_for_profit_organization]            = to_bool(xt(ps, './isForProfitOrganization'))
        h[:is_nonprofit_organization]             = to_bool(xt(ps, './isNonprofitOrganization'))
        h[:is_other_not_for_profit_organization]  = to_bool(xt(ps, './isOtherNotForProfitOrganization'))
      end
    end

    # Educational Entity
    ee = sd.at_xpath('.//typeOfEducationalEntity')
    if ee
      h[:is_1862_land_grant_college]                             = to_bool(xt(ee, './is1862LandGrantCollege'))
      h[:is_1890_land_grant_college]                             = to_bool(xt(ee, './is1890LandGrantCollege'))
      h[:is_1994_land_grant_college]                             = to_bool(xt(ee, './is1994LandGrantCollege'))
      h[:is_historically_black_college_or_university]            = to_bool(xt(ee, './isHistoricallyBlackCollegeOrUniversity'))
      h[:is_minority_institution]                                = to_bool(xt(ee, './isMinorityInstitution'))
      h[:is_private_university_or_college]                       = to_bool(xt(ee, './isPrivateUniversityOrCollege'))
      h[:is_school_of_forestry]                                  = to_bool(xt(ee, './isSchoolOfForestry'))
      h[:is_state_controlled_institution_of_higher_learning]     = to_bool(xt(ee, './isStateControlledInstitutionofHigherLearning'))
      h[:is_tribal_college]                                      = to_bool(xt(ee, './isTribalCollege'))
      h[:is_veterinary_college]                                  = to_bool(xt(ee, './isVeterinaryCollege'))
      h[:is_alaskan_native_servicing_institution]                = to_bool(xt(ee, './isAlaskanNativeServicingInstitution'))
      h[:is_native_hawaiian_servicing_institution]               = to_bool(xt(ee, './isNativeHawaiianServicingInstitution'))
    end

    # Government Entity
    ge = sd.at_xpath('.//typeOfGovernmentEntity')
    if ge
      h[:is_airport_authority]                       = to_bool(xt(ge, './isAirportAuthority'))
      h[:is_council_of_governments]                  = to_bool(xt(ge, './isCouncilOfGovernments'))
      h[:is_housing_authorities_public_or_tribal]    = to_bool(xt(ge, './isHousingAuthoritiesPublicOrTribal'))
      h[:is_interstate_entity]                       = to_bool(xt(ge, './isInterstateEntity'))
      h[:is_planning_commission]                     = to_bool(xt(ge, './isPlanningCommission'))
      h[:is_port_authority]                           = to_bool(xt(ge, './isPortAuthority'))
      h[:is_transit_authority]                        = to_bool(xt(ge, './isTransitAuthority'))
    end

    # Relationship with Federal Government
    rf = sd.at_xpath('.//vendorRelationshipWithFederalGovernment')
    if rf
      h[:receives_contracts]            = to_bool(xt(rf, './receivesContracts'))
      h[:receives_grants]               = to_bool(xt(rf, './receivesGrants'))
      h[:receives_contracts_and_grants] = to_bool(xt(rf, './receivesContractsAndGrants'))
    end

    h.compact
  end

  # Extract treasury accounts (returns array of hashes)
  def extract_treasury_accounts(cn)
    accounts = []
    cn.xpath('.//listOfTreasuryAccounts/treasuryAccount').each do |ta|
      acc = {
        agency_identifier:                    xt(ta, './/treasuryAccountSymbol/agencyIdentifier'),
        main_account_code:                    xt(ta, './/treasuryAccountSymbol/mainAccountCode'),
        sub_account_code:                     xt(ta, './/treasuryAccountSymbol/subAccountCode'),
        sub_level_prefix_code:                xt(ta, './/treasuryAccountSymbol/subLevelPrefixCode'),
        allocation_transfer_agency_identifier: xt(ta, './/treasuryAccountSymbol/allocationTransferAgencyIdentifier'),
        beginning_period_of_availability:     xt(ta, './/treasuryAccountSymbol/beginningPeriodOfAvailability'),
        ending_period_of_availability:        xt(ta, './/treasuryAccountSymbol/endingPeriodOfAvailability'),
        availability_type_code:               xt(ta, './/treasuryAccountSymbol/availabilityTypeCode'),
        initiative:                           xt(ta, './initiative'),
      }.compact
      accounts << acc unless acc.empty?
    end
    accounts
  end
end
