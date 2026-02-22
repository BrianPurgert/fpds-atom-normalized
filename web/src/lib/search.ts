import { createServerFn } from '@tanstack/react-start'
import { createClient } from '@supabase/supabase-js'

function getSupabase() {
  return createClient(
    process.env.VITE_SUPABASE_URL!,
    process.env.VITE_SUPABASE_ANON_KEY!,
  )
}

export type SortField =
  | 'atom_feed_modified_date'
  | 'signed_date'
  | 'obligated_amount'
  | 'effective_date'
  | 'created_date'
  | 'piid'

export type SortDir = 'desc' | 'asc'

export type SearchParams = {
  q?: string
  piid?: string
  solicitationId?: string
  description?: string
  agency?: string
  vendor?: string
  naics?: string
  psc?: string
  state?: string
  zipCode?: string
  congressionalDistrict?: string
  dateFrom?: string
  dateTo?: string
  modDateFrom?: string
  modDateTo?: string
  amountMin?: string
  amountMax?: string
  setAside?: string
  extentCompeted?: string
  contractFinancing?: string
  performanceBasedService?: string
  multiYear?: string
  reasonForModification?: string
  sortField?: SortField
  sortDir?: SortDir
  page?: number
  limit?: number
}

export type ContractResult = {
  id: number
  piid: string
  modification_number: string | null
  obligated_amount: number | null
  base_and_all_options_value: number | null
  effective_date: string | null
  signed_date: string | null
  atom_feed_modified_date: string | null
  created_date: string | null
  description_of_requirement: string | null
  action_type_code: string | null
  action_type_description: string | null
  pricing_type_code: string | null
  pricing_type_description: string | null
  extent_competed: string | null
  type_of_set_aside: string | null
  pop_street_address: string | null
  pop_city: string | null
  pop_state_code: string | null
  pop_zip_code: string | null
  pop_country_code: string | null
  pop_congressional_district: string | null
  record_type: string | null
  vendor_name: string | null
  uei_sam: string | null
  agency_name: string | null
  agency_code: string | null
  psc_code: string | null
  psc_description: string | null
  naics_code: string | null
  naics_description: string | null
  funding_agency_name: string | null
  contracting_office_name: string | null
  funding_office_name: string | null
  solicitation_id: string | null
  referenced_idv_piid: string | null
  referenced_idv_mod_number: string | null
  number_of_offers_received: number | null
  contract_financing: string | null
  reason_for_modification: string | null
  current_completion_date: string | null
  ultimate_completion_date: string | null
  base_and_exercised_options_value: number | null
  total_obligated_amount: number | null
  performance_based_service_contract: string | null
  multi_year_contract: string | null
  consolidated_contract: string | null
  national_interest_action_code: string | null
  cost_or_pricing_data: string | null
  commercial_item_acquisition_procedures: string | null
  fair_opportunity_limited_sources: string | null
  reason_not_competed: string | null
  country_of_origin: string | null
}

export type SearchResponse = {
  results: ContractResult[]
  total: number
  page: number
  limit: number
  totalPages: number
  sortField: SortField
  sortDir: SortDir
}

export const searchContracts = createServerFn({ method: 'GET' })
  .inputValidator((params: SearchParams) => {
    const s = (v: any): string | undefined => v == null ? undefined : String(v)
    return {
      ...params,
      q: s(params.q),
      piid: s(params.piid),
      solicitationId: s(params.solicitationId),
      description: s(params.description),
      agency: s(params.agency),
      vendor: s(params.vendor),
      naics: s(params.naics),
      psc: s(params.psc),
      state: s(params.state),
      zipCode: s(params.zipCode),
      congressionalDistrict: s(params.congressionalDistrict),
      dateFrom: s(params.dateFrom),
      dateTo: s(params.dateTo),
      modDateFrom: s(params.modDateFrom),
      modDateTo: s(params.modDateTo),
      amountMin: s(params.amountMin),
      amountMax: s(params.amountMax),
      setAside: s(params.setAside),
      extentCompeted: s(params.extentCompeted),
      contractFinancing: s(params.contractFinancing),
      performanceBasedService: s(params.performanceBasedService),
      multiYear: s(params.multiYear),
      reasonForModification: s(params.reasonForModification),
      sortField: s(params.sortField) as SortField | undefined,
      sortDir: s(params.sortDir) as SortDir | undefined,
    } as SearchParams
  })
  .handler(async ({ data: params }): Promise<SearchResponse> => {
    const db = getSupabase()
    const page = params.page ?? 1
    const limit = Math.min(params.limit ?? 25, 100)
    const offset = (page - 1) * limit
    const sortField: SortField = params.sortField ?? 'atom_feed_modified_date'
    const sortDir: SortDir = params.sortDir ?? 'desc'

    const emptyResult = { results: [], total: 0, page, limit, totalPages: 0, sortField, sortDir }

    // Pre-resolve dimension table lookups (async) so we can apply filters synchronously
    type DimIds = { agencyIds?: number[]; vendorIds?: number[]; naicsIds?: number[]; pscIds?: number[] }
    const dim: DimIds = {}

    if (params.agency && params.agency.trim()) {
      const { data: agencies } = await db
        .from('fpds_agencies')
        .select('id')
        .or(`agency_code.eq.${params.agency.trim()},agency_name.ilike.%${params.agency.trim()}%`)
      if (agencies && agencies.length > 0) {
        dim.agencyIds = agencies.map((a: any) => a.id)
      } else {
        return emptyResult
      }
    }
    if (params.vendor && params.vendor.trim()) {
      const { data: vendors } = await db
        .from('fpds_vendors')
        .select('id')
        .or(`uei_sam.eq.${params.vendor.trim()},vendor_name.ilike.%${params.vendor.trim()}%`)
        .limit(500)
      if (vendors && vendors.length > 0) {
        dim.vendorIds = vendors.map((v: any) => v.id)
      } else {
        return emptyResult
      }
    }
    if (params.naics && params.naics.trim()) {
      const { data: naics } = await db
        .from('fpds_naics_codes')
        .select('id')
        .or(`naics_code.eq.${params.naics.trim()},naics_description.ilike.%${params.naics.trim()}%`)
      if (naics && naics.length > 0) {
        dim.naicsIds = naics.map((n: any) => n.id)
      } else {
        return emptyResult
      }
    }
    if (params.psc && params.psc.trim()) {
      const { data: pscs } = await db
        .from('fpds_product_or_service_codes')
        .select('id')
        .or(`psc_code.eq.${params.psc.trim()},psc_description.ilike.%${params.psc.trim()}%`)
      if (pscs && pscs.length > 0) {
        dim.pscIds = pscs.map((p: any) => p.id)
      } else {
        return emptyResult
      }
    }

    // Synchronous filter application (no await, so Supabase thenable won't resolve)
    function applyFilters(q: any) {
      if (params.q && params.q.trim()) {
        const term = params.q.trim()
        
        // Use Full Text Search (FTS) if the term is substantial (>= 3 chars)
        // This utilizes the 'fts_vector' column and GIN index for high performance
        if (term.length >= 3) {
          q = q.textSearch('fts_vector', term, { 
            config: 'english',
            type: 'websearch' 
          })
        } else {
          // Fallback for short terms: search PIID/Solicitation directly using btree indices
          const filters: string[] = []
          if (term.length >= 2) {
            filters.push(`piid.ilike.%${term}%`)
            filters.push(`solicitation_id.ilike.%${term}%`)
          } else {
            // 1-char terms use high-selectivity equality match
            filters.push(`piid.eq.${term.toUpperCase()}`)
          }
          q = q.or(filters.join(','))
        }
      }
      if (params.piid && params.piid.trim()) q = q.ilike('piid', `%${params.piid.trim()}%`)
      if (params.solicitationId && params.solicitationId.trim()) q = q.ilike('solicitation_id', `%${params.solicitationId.trim()}%`)
      if (params.description && params.description.trim()) q = q.ilike('description_of_requirement', `%${params.description.trim()}%`)
      if (dim.agencyIds) q = q.in('agency_id', dim.agencyIds)
      if (dim.vendorIds) q = q.in('vendor_id', dim.vendorIds)
      if (dim.naicsIds) q = q.in('naics_code_id', dim.naicsIds)
      if (dim.pscIds) q = q.in('product_or_service_code_id', dim.pscIds)
      if (params.state && params.state.trim()) q = q.eq('pop_state_code', params.state.trim().toUpperCase())
      if (params.zipCode && params.zipCode.trim()) q = q.ilike('pop_zip_code', `${params.zipCode.trim()}%`)
      if (params.congressionalDistrict && params.congressionalDistrict.trim()) q = q.eq('pop_congressional_district', params.congressionalDistrict.trim())
      if (params.dateFrom) q = q.gte('signed_date', params.dateFrom)
      if (params.dateTo) q = q.lte('signed_date', params.dateTo)
      if (params.modDateFrom) q = q.gte('atom_feed_modified_date', params.modDateFrom)
      if (params.modDateTo) q = q.lte('atom_feed_modified_date', params.modDateTo + 'T23:59:59')
      if (params.amountMin) q = q.gte('obligated_amount', parseFloat(params.amountMin))
      if (params.amountMax) q = q.lte('obligated_amount', parseFloat(params.amountMax))
      if (params.setAside && params.setAside.trim()) q = q.eq('type_of_set_aside', params.setAside.trim())
      if (params.extentCompeted && params.extentCompeted.trim()) q = q.eq('extent_competed', params.extentCompeted.trim())
      if (params.contractFinancing && params.contractFinancing.trim()) q = q.eq('contract_financing', params.contractFinancing.trim())
      if (params.performanceBasedService && params.performanceBasedService.trim()) q = q.eq('performance_based_service_contract', params.performanceBasedService.trim())
      if (params.multiYear && params.multiYear.trim()) q = q.eq('multi_year_contract', params.multiYear.trim())
      if (params.reasonForModification && params.reasonForModification.trim()) {
        const codes = params.reasonForModification.split(',').map(s => s.trim()).filter(Boolean)
        if (codes.length > 0) {
          q = q.in('reason_for_modification', codes)
        }
      }
      return q
    }

    // Phase 1: lightweight count query (no joins, no data) - use estimated for speed
    let total = 0
    try {
      const countQuery = applyFilters(
        db.from('fpds_contract_actions').select('id', { count: 'estimated', head: true })
      )
      const { count, error } = await countQuery
      if (error) {
        console.error('Count error (estimated):', error)
        // Fallback to a very large number if count fails, to allow some pagination
        total = 1000000
      } else {
        total = count ?? 0
      }
    } catch (err) {
      console.error('Count exception:', err)
      total = 1000000
    }

    if (total === 0 && !params.q && !params.piid) {
       // Only return empty if we're sure there's no data
       // But if we have filters, we should try the data query anyway
    }

    // Phase 2: data query with joins (no count)
    const dataQuery = applyFilters(db
      .from('fpds_contract_actions')
      .select(`
        id, piid, modification_number, obligated_amount, base_and_all_options_value,
        base_and_exercised_options_value, total_obligated_amount,
        effective_date, signed_date, atom_feed_modified_date, created_date,
        description_of_requirement, action_type_code, action_type_description,
        pricing_type_code, pricing_type_description, extent_competed, type_of_set_aside,
        pop_street_address, pop_city, pop_state_code, pop_zip_code, pop_country_code,
        pop_congressional_district, record_type, solicitation_id,
        referenced_idv_piid, referenced_idv_mod_number, number_of_offers_received,
        contract_financing, reason_for_modification, current_completion_date,
        ultimate_completion_date, performance_based_service_contract, multi_year_contract,
        consolidated_contract, national_interest_action_code, cost_or_pricing_data,
        commercial_item_acquisition_procedures, fair_opportunity_limited_sources,
        reason_not_competed, country_of_origin,
        fpds_vendors!fpds_contract_actions_vendor_id_fkey (vendor_name, uei_sam),
        agency:fpds_agencies!fpds_contract_actions_agency_id_fkey (agency_name, agency_code),
        funding_agency:fpds_agencies!fpds_contract_actions_funding_agency_id_fkey (agency_name),
        contracting_office:fpds_government_offices!fpds_contract_actions_contracting_office_id_fkey (office_name),
        funding_office:fpds_government_offices!fpds_contract_actions_funding_office_id_fkey (office_name),
        fpds_product_or_service_codes!fpds_contract_actions_product_or_service_code_id_fkey (psc_code, psc_description),
        fpds_naics_codes!fpds_contract_actions_naics_code_id_fkey (naics_code, naics_description)
      `)
    )
      .order(sortField, { ascending: sortDir === 'asc', nullsFirst: false })
      .range(offset, offset + limit - 1)

    const dataResult = await dataQuery

    if (dataResult.error) {
      console.error('Search error:', dataResult.error)
      throw new Error(`Search failed: ${dataResult.error.message}`)
    }

    const results: ContractResult[] = (dataResult.data ?? []).map((row: any) => ({
      id: row.id,
      piid: row.piid,
      modification_number: row.modification_number,
      obligated_amount: row.obligated_amount,
      base_and_all_options_value: row.base_and_all_options_value,
      base_and_exercised_options_value: row.base_and_exercised_options_value,
      total_obligated_amount: row.total_obligated_amount,
      effective_date: row.effective_date,
      signed_date: row.signed_date,
      atom_feed_modified_date: row.atom_feed_modified_date,
      created_date: row.created_date,
      description_of_requirement: row.description_of_requirement,
      action_type_code: row.action_type_code,
      action_type_description: row.action_type_description,
      pricing_type_code: row.pricing_type_code,
      pricing_type_description: row.pricing_type_description,
      extent_competed: row.extent_competed,
      type_of_set_aside: row.type_of_set_aside,
      pop_street_address: row.pop_street_address,
      pop_city: row.pop_city,
      pop_state_code: row.pop_state_code,
      pop_zip_code: row.pop_zip_code,
      pop_country_code: row.pop_country_code,
      pop_congressional_district: row.pop_congressional_district,
      record_type: row.record_type,
      solicitation_id: row.solicitation_id,
      referenced_idv_piid: row.referenced_idv_piid,
      referenced_idv_mod_number: row.referenced_idv_mod_number,
      number_of_offers_received: row.number_of_offers_received,
      contract_financing: row.contract_financing,
      reason_for_modification: row.reason_for_modification,
      current_completion_date: row.current_completion_date,
      ultimate_completion_date: row.ultimate_completion_date,
      performance_based_service_contract: row.performance_based_service_contract,
      multi_year_contract: row.multi_year_contract,
      consolidated_contract: row.consolidated_contract,
      national_interest_action_code: row.national_interest_action_code,
      cost_or_pricing_data: row.cost_or_pricing_data,
      commercial_item_acquisition_procedures: row.commercial_item_acquisition_procedures,
      fair_opportunity_limited_sources: row.fair_opportunity_limited_sources,
      reason_not_competed: row.reason_not_competed,
      country_of_origin: row.country_of_origin,
      vendor_name: row.fpds_vendors?.vendor_name ?? null,
      uei_sam: row.fpds_vendors?.uei_sam ?? null,
      agency_name: row.agency?.agency_name ?? null,
      agency_code: row.agency?.agency_code ?? null,
      psc_code: row.fpds_product_or_service_codes?.psc_code ?? null,
      psc_description: row.fpds_product_or_service_codes?.psc_description ?? null,
      naics_code: row.fpds_naics_codes?.naics_code ?? null,
      naics_description: row.fpds_naics_codes?.naics_description ?? null,
      funding_agency_name: row.funding_agency?.agency_name ?? null,
      contracting_office_name: row.contracting_office?.office_name ?? null,
      funding_office_name: row.funding_office?.office_name ?? null,
    }))

    return {
      results,
      total,
      page,
      limit,
      totalPages: Math.ceil(total / limit),
      sortField,
      sortDir,
    }
  })

let filterOptionsCache: any = null
let lastCacheTime = 0
const CACHE_TTL = 3600 * 1000 // 1 hour

export const getFilterOptions = createServerFn({ method: 'GET' })
  .handler(async () => {
    if (filterOptionsCache && Date.now() - lastCacheTime < CACHE_TTL) {
      return filterOptionsCache
    }

    const db = getSupabase()

    const [agencies, setAsides, states, extentCompeted] = await Promise.all([
      db.from('fpds_agencies').select('agency_code, agency_name').order('agency_name').limit(500),
      db.from('fpds_contract_actions').select('type_of_set_aside').not('type_of_set_aside', 'is', null).limit(2000),
      db.from('fpds_contract_actions').select('pop_state_code').not('pop_state_code', 'is', null).limit(10000),
      db.from('fpds_contract_actions').select('extent_competed').not('extent_competed', 'is', null).limit(2000),
    ])

    const uniqueSetAsides = [...new Set((setAsides.data ?? []).map((r: any) => r.type_of_set_aside).filter(Boolean))].sort()
    const uniqueStates = [...new Set((states.data ?? []).map((r: any) => r.pop_state_code).filter(Boolean))].sort()
    const uniqueExtentCompeted = [...new Set((extentCompeted.data ?? []).map((r: any) => r.extent_competed).filter(Boolean))].sort()

    const result = {
      agencies: agencies.data ?? [],
      setAsides: uniqueSetAsides as string[],
      states: uniqueStates as string[],
      extentCompeted: uniqueExtentCompeted as string[],
    }

    filterOptionsCache = result
    lastCacheTime = Date.now()
    return result
  })
