import { createFileRoute, useNavigate } from '@tanstack/react-router'
import { useState } from 'react'
import { searchContracts, getFilterOptions } from '~/lib/search'
import type { SearchResponse, ContractResult, SortField, SortDir } from '~/lib/search'

type IndexSearchParams = {
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
  sortField?: SortField
  sortDir?: SortDir
  page?: number
}

export const Route = createFileRoute('/')({
  validateSearch: (search: Record<string, unknown>): IndexSearchParams => ({
    q: (search.q as string) || undefined,
    piid: (search.piid as string) || undefined,
    solicitationId: (search.solicitationId as string) || undefined,
    description: (search.description as string) || undefined,
    agency: (search.agency as string) || undefined,
    vendor: (search.vendor as string) || undefined,
    naics: (search.naics as string) || undefined,
    psc: (search.psc as string) || undefined,
    state: (search.state as string) || undefined,
    zipCode: (search.zipCode as string) || undefined,
    congressionalDistrict: (search.congressionalDistrict as string) || undefined,
    dateFrom: (search.dateFrom as string) || undefined,
    dateTo: (search.dateTo as string) || undefined,
    modDateFrom: (search.modDateFrom as string) || undefined,
    modDateTo: (search.modDateTo as string) || undefined,
    amountMin: (search.amountMin as string) || undefined,
    amountMax: (search.amountMax as string) || undefined,
    setAside: (search.setAside as string) || undefined,
    extentCompeted: (search.extentCompeted as string) || undefined,
    contractFinancing: (search.contractFinancing as string) || undefined,
    performanceBasedService: (search.performanceBasedService as string) || undefined,
    multiYear: (search.multiYear as string) || undefined,
    sortField: (search.sortField as SortField) || undefined,
    sortDir: (search.sortDir as SortDir) || undefined,
    page: search.page ? Number(search.page) : undefined,
  }),
  loaderDeps: ({ search }) => search,
  loader: async ({ deps }) => {
    const hasSearch = Object.values(deps).some(v => v !== undefined)
    const [filterOptions, searchResults] = await Promise.all([
      getFilterOptions(),
      hasSearch ? searchContracts({ data: { ...deps, page: deps.page ?? 1, limit: 25 } }) : null,
    ])
    return { filterOptions, searchResults }
  },
  component: SearchPage,
})

function formatCurrency(amount: number | null): string {
  if (amount === null || amount === undefined) return ''
  return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 2 }).format(amount)
}

function formatDate(dateStr: string | null): string {
  if (!dateStr) return ''
  try {
    return new Date(dateStr).toLocaleDateString('en-US', { year: 'numeric', month: '2-digit', day: '2-digit' })
  } catch {
    return dateStr
  }
}

function formatDateTime(dateStr: string | null): string {
  if (!dateStr) return ''
  try {
    const d = new Date(dateStr)
    return d.toLocaleDateString('en-US', { year: 'numeric', month: '2-digit', day: '2-digit' }) +
      ' ' + d.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false })
  } catch {
    return dateStr
  }
}

const SORT_LABELS: Record<SortField, string> = {
  atom_feed_modified_date: 'Last Modified Date',
  signed_date: 'Date Signed',
  obligated_amount: 'Obligated Amount',
  effective_date: 'Effective Date',
  created_date: 'Created Date',
  piid: 'Contract ID (PIID)',
}

const EXTENT_COMPETED_LABELS: Record<string, string> = {
  A: 'Full and Open Competition',
  B: 'Not Available for Competition',
  C: 'Not Competed',
  D: 'Full and Open after Exclusion of Sources',
  E: 'Follow On to Competed Action',
  F: 'Competed under SAP',
  G: 'Not Competed under SAP',
  CDO: 'Competitive Delivery Order',
  NDO: 'Non-Competitive Delivery Order',
}

/* SVG Icons */
function SearchIcon() {
  return (
    <svg className="icon" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5">
      <circle cx="7" cy="7" r="5" />
      <line x1="11" y1="11" x2="15" y2="15" />
    </svg>
  )
}

function FilterIcon() {
  return (
    <svg className="icon" viewBox="0 0 16 16" fill="currentColor">
      <path d="M1 2h14l-5.5 6.5V14l-3-1.5V8.5L1 2z" />
    </svg>
  )
}

function ChevronDown() {
  return (
    <svg className="icon" viewBox="0 0 16 16" fill="currentColor" style={{ width: 10, height: 10 }}>
      <path d="M4 6l4 4 4-4" fill="none" stroke="currentColor" strokeWidth="2" />
    </svg>
  )
}

function ChevronRight() {
  return (
    <svg className="icon" viewBox="0 0 16 16" fill="currentColor" style={{ width: 10, height: 10 }}>
      <path d="M6 4l4 4-4 4" fill="none" stroke="currentColor" strokeWidth="2" />
    </svg>
  )
}

function SortAscIcon() {
  return (
    <svg style={{ width: 10, height: 10, display: 'inline', verticalAlign: 'middle', marginLeft: 2 }} viewBox="0 0 10 10">
      <path d="M5 2L9 8H1z" fill="currentColor" />
    </svg>
  )
}

function SortDescIcon() {
  return (
    <svg style={{ width: 10, height: 10, display: 'inline', verticalAlign: 'middle', marginLeft: 2 }} viewBox="0 0 10 10">
      <path d="M5 8L1 2h8z" fill="currentColor" />
    </svg>
  )
}

function DocIcon() {
  return (
    <svg className="icon" viewBox="0 0 16 16" fill="none" stroke="#0558a5" strokeWidth="1">
      <rect x="2" y="1" width="10" height="14" rx="1" />
      <line x1="4" y1="4" x2="10" y2="4" />
      <line x1="4" y1="6.5" x2="10" y2="6.5" />
      <line x1="4" y1="9" x2="8" y2="9" />
    </svg>
  )
}

function SearchPage() {
  const { filterOptions, searchResults } = Route.useLoaderData()
  const search = Route.useSearch()
  const navigate = useNavigate()
  const [showFilters, setShowFilters] = useState(
    !!(search.agency || search.vendor || search.naics || search.psc || search.state ||
       search.dateFrom || search.dateTo || search.amountMin || search.amountMax || search.setAside ||
       search.zipCode || search.congressionalDistrict || search.modDateFrom || search.modDateTo ||
       search.extentCompeted || search.piid || search.solicitationId || search.description)
  )

  const [formState, setFormState] = useState<IndexSearchParams>({
    q: search.q ?? '',
    piid: search.piid ?? '',
    solicitationId: search.solicitationId ?? '',
    description: search.description ?? '',
    agency: search.agency ?? '',
    vendor: search.vendor ?? '',
    naics: search.naics ?? '',
    psc: search.psc ?? '',
    state: search.state ?? '',
    zipCode: search.zipCode ?? '',
    congressionalDistrict: search.congressionalDistrict ?? '',
    dateFrom: search.dateFrom ?? '',
    dateTo: search.dateTo ?? '',
    modDateFrom: search.modDateFrom ?? '',
    modDateTo: search.modDateTo ?? '',
    amountMin: search.amountMin ?? '',
    amountMax: search.amountMax ?? '',
    setAside: search.setAside ?? '',
    extentCompeted: search.extentCompeted ?? '',
    sortField: search.sortField,
    sortDir: search.sortDir,
  })

  const buildParams = (pageNum?: number) => {
    const params: Record<string, string | number | undefined> = {}
    if (formState.q) params.q = formState.q
    if (formState.piid) params.piid = formState.piid
    if (formState.solicitationId) params.solicitationId = formState.solicitationId
    if (formState.description) params.description = formState.description
    if (formState.agency) params.agency = formState.agency
    if (formState.vendor) params.vendor = formState.vendor
    if (formState.naics) params.naics = formState.naics
    if (formState.psc) params.psc = formState.psc
    if (formState.state) params.state = formState.state
    if (formState.zipCode) params.zipCode = formState.zipCode
    if (formState.congressionalDistrict) params.congressionalDistrict = formState.congressionalDistrict
    if (formState.dateFrom) params.dateFrom = formState.dateFrom
    if (formState.dateTo) params.dateTo = formState.dateTo
    if (formState.modDateFrom) params.modDateFrom = formState.modDateFrom
    if (formState.modDateTo) params.modDateTo = formState.modDateTo
    if (formState.amountMin) params.amountMin = formState.amountMin
    if (formState.amountMax) params.amountMax = formState.amountMax
    if (formState.setAside) params.setAside = formState.setAside
    if (formState.extentCompeted) params.extentCompeted = formState.extentCompeted
    if (formState.sortField && formState.sortField !== 'atom_feed_modified_date') params.sortField = formState.sortField
    if (formState.sortDir && formState.sortDir !== 'desc') params.sortDir = formState.sortDir
    if (pageNum && pageNum > 1) params.page = pageNum
    return params
  }

  const doSearch = (pageNum?: number) => {
    navigate({ to: '/', search: buildParams(pageNum) })
  }

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault()
    doSearch()
  }

  const goToPage = (p: number) => {
    const params = buildParams(p)
    // Preserve existing sort from search results
    if (search.sortField) params.sortField = search.sortField
    if (search.sortDir) params.sortDir = search.sortDir
    navigate({ to: '/', search: params })
  }

  const changeSort = (field: SortField) => {
    const currentField = formState.sortField ?? 'atom_feed_modified_date'
    const currentDir = formState.sortDir ?? 'desc'
    let newDir: SortDir = 'desc'
    if (field === currentField) {
      newDir = currentDir === 'desc' ? 'asc' : 'desc'
    }
    setFormState(s => ({ ...s, sortField: field, sortDir: newDir }))
    const params = buildParams()
    if (field !== 'atom_feed_modified_date') params.sortField = field
    else delete params.sortField
    if (newDir !== 'desc') params.sortDir = newDir
    else delete params.sortDir
    delete params.page
    navigate({ to: '/', search: params })
  }

  const clearSearch = () => {
    setFormState({
      q: '', piid: '', solicitationId: '', description: '',
      agency: '', vendor: '', naics: '', psc: '', state: '',
      zipCode: '', congressionalDistrict: '',
      dateFrom: '', dateTo: '', modDateFrom: '', modDateTo: '',
      amountMin: '', amountMax: '', setAside: '', extentCompeted: '',
      sortField: undefined, sortDir: undefined,
    })
    navigate({ to: '/', search: {} })
  }

  const hasActiveSearch = Object.values(search).some(v => v !== undefined)
  const activeFilterCount = [
    search.agency, search.vendor, search.naics, search.psc, search.state,
    search.zipCode, search.congressionalDistrict,
    search.dateFrom, search.dateTo, search.modDateFrom, search.modDateTo,
    search.amountMin, search.amountMax, search.setAside, search.extentCompeted,
    search.piid, search.solicitationId, search.description,
  ].filter(Boolean).length

  return (
    <div style={{ padding: '6px 8px', flex: 1 }}>
      {/* Search Box */}
      <table className="box">
        <tbody>
          <tr>
            <td className="box-heading">
              <SearchIcon /> ezSearch — Contract Awards
            </td>
          </tr>
          <tr>
            <td style={{ padding: '8px', background: '#fff' }}>
              <form onSubmit={handleSearch}>
                <table>
                  <tbody>
                    <tr>
                      <td>
                        <span className="search_heading">Quick Search (searches PIID, Solicitation ID, and Description):</span>
                      </td>
                    </tr>
                    <tr>
                      <td>
                        <input
                          type="text"
                          value={formState.q ?? ''}
                          onChange={e => setFormState(s => ({ ...s, q: e.target.value }))}
                          size={80}
                          maxLength={2048}
                          style={{ fontSize: '9pt', padding: '2px 4px', border: '1px solid #999' }}
                          placeholder="Type any keyword, PIID, solicitation ID, or description..."
                        />
                        {' '}
                        <button type="submit" className="go-btn">Search</button>
                        {' '}
                        <button type="button" className="clear-btn" onClick={clearSearch}>Clear All</button>
                        {' '}
                        <button
                          type="button"
                          className="clear-btn"
                          onClick={() => setShowFilters(!showFilters)}
                          style={{ marginLeft: 4 }}
                        >
                          <FilterIcon />
                          {showFilters ? ' Hide' : ' Show'} Advanced Filters
                          {activeFilterCount > 0 && ` (${activeFilterCount})`}
                          {showFilters ? <ChevronDown /> : <ChevronRight />}
                        </button>
                      </td>
                    </tr>
                  </tbody>
                </table>

                {/* Advanced Filters */}
                {showFilters && (
                  <table className="filter-box" style={{ marginTop: 6 }}>
                    <tbody>
                      <tr style={{ background: '#d2e4fc' }}>
                        <td colSpan={6}><strong style={{ fontSize: '8pt' }}>Contract Identification</strong></td>
                      </tr>
                      <tr>
                        <td><label>PIID (Contract ID):</label></td>
                        <td>
                          <input
                            type="text"
                            value={formState.piid ?? ''}
                            onChange={e => setFormState(s => ({ ...s, piid: e.target.value }))}
                            size={25}
                            placeholder="e.g. FA8732 or W911NF"
                          />
                        </td>
                        <td><label>Solicitation ID:</label></td>
                        <td>
                          <input
                            type="text"
                            value={formState.solicitationId ?? ''}
                            onChange={e => setFormState(s => ({ ...s, solicitationId: e.target.value }))}
                            size={25}
                            placeholder="e.g. W912HQ"
                          />
                        </td>
                        <td><label>Description:</label></td>
                        <td>
                          <input
                            type="text"
                            value={formState.description ?? ''}
                            onChange={e => setFormState(s => ({ ...s, description: e.target.value }))}
                            size={30}
                            placeholder="e.g. aircraft maintenance"
                          />
                        </td>
                      </tr>

                      <tr style={{ background: '#d2e4fc' }}>
                        <td colSpan={6}><strong style={{ fontSize: '8pt' }}>Contracting Agency / Vendor</strong></td>
                      </tr>
                      <tr>
                        <td><label>Agency:</label></td>
                        <td>
                          <select
                            value={formState.agency ?? ''}
                            onChange={e => setFormState(s => ({ ...s, agency: e.target.value }))}
                            style={{ width: 220 }}
                          >
                            <option value="">-- All Agencies --</option>
                            {filterOptions.agencies.map((a: any) => (
                              <option key={a.agency_code} value={a.agency_code}>
                                {a.agency_name} ({a.agency_code})
                              </option>
                            ))}
                          </select>
                        </td>
                        <td><label>Vendor Name / UEI:</label></td>
                        <td colSpan={3}>
                          <input
                            type="text"
                            value={formState.vendor ?? ''}
                            onChange={e => setFormState(s => ({ ...s, vendor: e.target.value }))}
                            size={40}
                            placeholder="e.g. Lockheed Martin or UEI"
                          />
                        </td>
                      </tr>

                      <tr style={{ background: '#d2e4fc' }}>
                        <td colSpan={6}><strong style={{ fontSize: '8pt' }}>Product / Service Classification</strong></td>
                      </tr>
                      <tr>
                        <td><label>NAICS Code:</label></td>
                        <td>
                          <input
                            type="text"
                            value={formState.naics ?? ''}
                            onChange={e => setFormState(s => ({ ...s, naics: e.target.value }))}
                            size={20}
                            placeholder="e.g. 541512"
                          />
                        </td>
                        <td><label>PSC Code:</label></td>
                        <td>
                          <input
                            type="text"
                            value={formState.psc ?? ''}
                            onChange={e => setFormState(s => ({ ...s, psc: e.target.value }))}
                            size={20}
                            placeholder="e.g. D306"
                          />
                        </td>
                        <td colSpan={2}></td>
                      </tr>

                      <tr style={{ background: '#d2e4fc' }}>
                        <td colSpan={6}><strong style={{ fontSize: '8pt' }}>Place of Performance</strong></td>
                      </tr>
                      <tr>
                        <td><label>State:</label></td>
                        <td>
                          <select
                            value={formState.state ?? ''}
                            onChange={e => setFormState(s => ({ ...s, state: e.target.value }))}
                          >
                            <option value="">-- All --</option>
                            {filterOptions.states.map((s: string) => (
                              <option key={s} value={s}>{s}</option>
                            ))}
                          </select>
                        </td>
                        <td><label>ZIP Code:</label></td>
                        <td>
                          <input
                            type="text"
                            value={formState.zipCode ?? ''}
                            onChange={e => setFormState(s => ({ ...s, zipCode: e.target.value }))}
                            size={10}
                            placeholder="e.g. 22030"
                          />
                        </td>
                        <td><label>Congressional Dist:</label></td>
                        <td>
                          <input
                            type="text"
                            value={formState.congressionalDistrict ?? ''}
                            onChange={e => setFormState(s => ({ ...s, congressionalDistrict: e.target.value }))}
                            size={6}
                            placeholder="e.g. 08"
                          />
                        </td>
                      </tr>

                      <tr style={{ background: '#d2e4fc' }}>
                        <td colSpan={6}><strong style={{ fontSize: '8pt' }}>Competition</strong></td>
                      </tr>
                      <tr>
                        <td><label>Set-Aside Type:</label></td>
                        <td>
                          <select
                            value={formState.setAside ?? ''}
                            onChange={e => setFormState(s => ({ ...s, setAside: e.target.value }))}
                          >
                            <option value="">-- All --</option>
                            {filterOptions.setAsides.map((s: string) => (
                              <option key={s} value={s}>{s}</option>
                            ))}
                          </select>
                        </td>
                        <td><label>Extent Competed:</label></td>
                        <td>
                          <select
                            value={formState.extentCompeted ?? ''}
                            onChange={e => setFormState(s => ({ ...s, extentCompeted: e.target.value }))}
                          >
                            <option value="">-- All --</option>
                            {filterOptions.extentCompeted.map((s: string) => (
                              <option key={s} value={s}>{s} {EXTENT_COMPETED_LABELS[s] ? `- ${EXTENT_COMPETED_LABELS[s]}` : ''}</option>
                            ))}
                          </select>
                        </td>
                        <td colSpan={2}></td>
                      </tr>

                      <tr style={{ background: '#d2e4fc' }}>
                        <td colSpan={6}><strong style={{ fontSize: '8pt' }}>Dates</strong></td>
                      </tr>
                      <tr>
                        <td><label>Signed Date From:</label></td>
                        <td>
                          <input type="date" value={formState.dateFrom ?? ''} onChange={e => setFormState(s => ({ ...s, dateFrom: e.target.value }))} />
                        </td>
                        <td><label>Signed Date To:</label></td>
                        <td>
                          <input type="date" value={formState.dateTo ?? ''} onChange={e => setFormState(s => ({ ...s, dateTo: e.target.value }))} />
                        </td>
                        <td colSpan={2}></td>
                      </tr>
                      <tr>
                        <td><label>Last Modified From:</label></td>
                        <td>
                          <input type="date" value={formState.modDateFrom ?? ''} onChange={e => setFormState(s => ({ ...s, modDateFrom: e.target.value }))} />
                        </td>
                        <td><label>Last Modified To:</label></td>
                        <td>
                          <input type="date" value={formState.modDateTo ?? ''} onChange={e => setFormState(s => ({ ...s, modDateTo: e.target.value }))} />
                        </td>
                        <td colSpan={2}></td>
                      </tr>

                      <tr style={{ background: '#d2e4fc' }}>
                        <td colSpan={6}><strong style={{ fontSize: '8pt' }}>Dollar Amounts</strong></td>
                      </tr>
                      <tr>
                        <td><label>Obligation Min ($):</label></td>
                        <td>
                          <input type="number" value={formState.amountMin ?? ''} onChange={e => setFormState(s => ({ ...s, amountMin: e.target.value }))} size={12} placeholder="0" />
                        </td>
                        <td><label>Obligation Max ($):</label></td>
                        <td>
                          <input type="number" value={formState.amountMax ?? ''} onChange={e => setFormState(s => ({ ...s, amountMax: e.target.value }))} size={12} placeholder="999999999" />
                        </td>
                        <td colSpan={2}></td>
                      </tr>

                      <tr>
                        <td colSpan={6} style={{ padding: '6px', textAlign: 'right', background: '#eef4ff' }}>
                          <button type="submit" className="go-btn">Search</button>
                          {' '}
                          <button type="button" className="clear-btn" onClick={clearSearch}>Clear All</button>
                        </td>
                      </tr>
                    </tbody>
                  </table>
                )}
              </form>
            </td>
          </tr>
        </tbody>
      </table>

      {/* Results */}
      {!hasActiveSearch ? (
        <WelcomeBox />
      ) : searchResults ? (
        <ResultsBox
          results={searchResults}
          currentPage={search.page ?? 1}
          goToPage={goToPage}
          sortField={searchResults.sortField}
          sortDir={searchResults.sortDir}
          changeSort={changeSort}
        />
      ) : null}
    </div>
  )
}

function WelcomeBox() {
  return (
    <table className="box" style={{ marginTop: 8 }}>
      <tbody>
        <tr>
          <td className="box-heading">Welcome</td>
        </tr>
        <tr>
          <td style={{ padding: 12, background: '#fff' }}>
            <table width="100%">
              <tbody>
                <tr>
                  <td>
                    <span className="search_heading" style={{ fontWeight: 'bold' }}>
                      Search Federal Contract Awards
                    </span>
                    <br /><br />
                    <span className="results_text">
                      Use the Quick Search bar above to find contracts by any keyword — it searches across PIID, Solicitation ID, and Description simultaneously.
                      Click "Show Advanced Filters" for precise filtering by agency, vendor, location, dates, dollar amounts, and more.
                    </span>
                    <br /><br />
                    <span className="results_text" style={{ fontStyle: 'italic' }}>
                      Results are sorted by Last Modified Date (newest first) by default. You can change the sort order using the column headers in the results.
                    </span>
                    <br /><br />
                    <table style={{ borderCollapse: 'collapse' }}>
                      <tbody>
                        <tr>
                          <td style={{ padding: '4px 12px 4px 0', verticalAlign: 'top' }}><SearchIcon /></td>
                          <td style={{ padding: '4px 0' }}>
                            <span className="results_title_text">Quick Search</span><br />
                            <span className="results_text">Type any keyword to search across PIID, Solicitation ID, and Description at once</span>
                          </td>
                        </tr>
                        <tr>
                          <td style={{ padding: '4px 12px 4px 0', verticalAlign: 'top' }}><FilterIcon /></td>
                          <td style={{ padding: '4px 0' }}>
                            <span className="results_title_text">Advanced Filters</span><br />
                            <span className="results_text">Filter by agency, vendor, NAICS/PSC codes, location, competition type, dates, and dollar amounts</span>
                          </td>
                        </tr>
                        <tr>
                          <td style={{ padding: '4px 12px 4px 0', verticalAlign: 'top' }}><DocIcon /></td>
                          <td style={{ padding: '4px 0' }}>
                            <span className="results_title_text">Detailed Records</span><br />
                            <span className="results_text">Click any contract to expand and view all available fields</span>
                          </td>
                        </tr>
                      </tbody>
                    </table>
                  </td>
                </tr>
              </tbody>
            </table>
          </td>
        </tr>
      </tbody>
    </table>
  )
}

function ResultsBox({ results, currentPage, goToPage, sortField, sortDir, changeSort }: {
  results: SearchResponse
  currentPage: number
  goToPage: (p: number) => void
  sortField: SortField
  sortDir: SortDir
  changeSort: (field: SortField) => void
}) {
  return (
    <div style={{ marginTop: 8 }}>
      <table className="box">
        <tbody>
          <tr>
            <td className="box-heading">
              <SearchIcon /> Search Results
            </td>
          </tr>
          <tr>
            <td className="paging-row">
              <span className="results_title_text">
                {results.total.toLocaleString()} results found
              </span>
              {results.totalPages > 1 && (
                <span className="results_text">
                  {' '} — Page {results.page} of {results.totalPages.toLocaleString()}
                  {' | '}
                  <PaginationLinks currentPage={currentPage} totalPages={results.totalPages} goToPage={goToPage} />
                </span>
              )}
            </td>
          </tr>
          <tr>
            <td style={{ padding: '4px 8px', background: '#eef4ff', fontSize: '8pt', fontFamily: 'arial, sans-serif' }}>
              <span style={{ fontWeight: 'bold' }}>Sort by: </span>
              {(Object.keys(SORT_LABELS) as SortField[]).map((field) => (
                <span key={field} style={{ marginRight: 8 }}>
                  <a
                    href="#"
                    onClick={(e) => { e.preventDefault(); changeSort(field) }}
                    className="link_text"
                    style={{
                      fontWeight: sortField === field ? 'bold' : 'normal',
                      textDecoration: sortField === field ? 'none' : 'underline',
                      color: sortField === field ? '#000' : '#0000ff',
                      fontSize: '8pt',
                    }}
                  >
                    {SORT_LABELS[field]}
                    {sortField === field && (sortDir === 'asc' ? <SortAscIcon /> : <SortDescIcon />)}
                  </a>
                </span>
              ))}
            </td>
          </tr>
        </tbody>
      </table>

      {results.results.length === 0 ? (
        <table className="resultbox2" style={{ marginTop: 4 }}>
          <tbody>
            <tr>
              <td style={{ padding: 12, textAlign: 'center' }}>
                <span className="warning_text">No contracts found matching your search criteria.</span>
              </td>
            </tr>
          </tbody>
        </table>
      ) : (
        results.results.map((contract, idx) => (
          <ContractRow key={contract.id} contract={contract} index={idx} />
        ))
      )}

      {results.totalPages > 1 && (
        <table className="box" style={{ marginTop: 4 }}>
          <tbody>
            <tr>
              <td className="paging-row" style={{ textAlign: 'center' }}>
                <PaginationLinks currentPage={currentPage} totalPages={results.totalPages} goToPage={goToPage} />
              </td>
            </tr>
          </tbody>
        </table>
      )}
    </div>
  )
}

function ContractRow({ contract, index }: { contract: ContractResult; index: number }) {
  const [expanded, setExpanded] = useState(false)
  const tableClass = index % 2 === 0 ? 'resultbox1' : 'resultbox2'

  return (
    <table className={tableClass} style={{ marginTop: index === 0 ? 4 : 0 }}>
      <tbody>
        <tr>
          <td style={{ padding: '4px 8px', width: '100%' }}>
            <table width="100%" style={{ borderCollapse: 'collapse' }}>
              <tbody>
                <tr>
                  <td style={{ verticalAlign: 'top', width: '55%' }}>
                    <span
                      style={{ cursor: 'pointer', color: '#0000ff', textDecoration: 'underline', fontSize: '8pt', fontWeight: 'bold' }}
                      onClick={() => setExpanded(!expanded)}
                    >
                      {expanded ? <ChevronDown /> : <ChevronRight />}
                      {' '}{contract.piid}
                      {contract.modification_number && contract.modification_number !== '0' ? ` / Mod ${contract.modification_number}` : ''}
                    </span>
                    {contract.description_of_requirement && (
                      <>
                        <br />
                        <span className="results_text" style={{ color: '#444' }}>
                          {contract.description_of_requirement.length > 120
                            ? contract.description_of_requirement.substring(0, 120) + '...'
                            : contract.description_of_requirement}
                        </span>
                      </>
                    )}
                    <br />
                    <span className="results_title_text">Vendor: </span>
                    <span className="results_text">{contract.vendor_name ?? 'N/A'}</span>
                    {contract.uei_sam && (
                      <>
                        <span className="results_title_text"> | UEI: </span>
                        <span className="results_text">{contract.uei_sam}</span>
                      </>
                    )}
                  </td>
                  <td style={{ verticalAlign: 'top', width: '45%', textAlign: 'right' }}>
                    <span className="results_title_text">Action Obligation: </span>
                    <span className="results_text" style={{ fontWeight: 'bold' }}>{formatCurrency(contract.obligated_amount)}</span>
                    <br />
                    <span className="results_title_text">Date Signed: </span>
                    <span className="results_text">{formatDate(contract.signed_date)}</span>
                    <br />
                    <span className="results_title_text">Last Modified: </span>
                    <span className="results_text">{formatDateTime(contract.atom_feed_modified_date)}</span>
                  </td>
                </tr>
                <tr>
                  <td colSpan={2}>
                    <span className="results_title_text">Agency: </span>
                    <span className="results_text">{contract.agency_name ?? 'N/A'}</span>
                    {contract.agency_code && (
                      <span className="results_text"> ({contract.agency_code})</span>
                    )}
                    {contract.contracting_office_name && (
                      <>
                        <span className="results_title_text"> | Office: </span>
                        <span className="results_text">{contract.contracting_office_name}</span>
                      </>
                    )}
                    {contract.action_type_description && (
                      <>
                        <span className="results_title_text"> | Action: </span>
                        <span className="results_text">{contract.action_type_description}</span>
                      </>
                    )}
                    {contract.naics_code && (
                      <>
                        <span className="results_title_text"> | NAICS: </span>
                        <span className="results_text">{contract.naics_code}</span>
                      </>
                    )}
                  </td>
                </tr>
              </tbody>
            </table>
          </td>
        </tr>

        {expanded && (
          <tr>
            <td style={{ padding: '2px 8px 6px 20px', borderTop: '1px solid #bbb' }}>
              <table width="100%" style={{ borderCollapse: 'collapse' }}>
                <tbody>
                  {/* Identification */}
                  <tr><td colSpan={2} style={{ padding: '4px 0 2px', borderBottom: '1px solid #ccc' }}><strong className="results_title_text" style={{ color: '#0558a5' }}>Identification</strong></td></tr>
                  <DetailRow label="PIID" value={contract.piid} label2="Modification Number" value2={contract.modification_number ?? 'N/A'} />
                  {contract.referenced_idv_piid && <DetailRow label="Referenced IDV PIID" value={contract.referenced_idv_piid} label2="IDV Mod Number" value2={contract.referenced_idv_mod_number ?? 'N/A'} />}
                  {contract.solicitation_id && <DetailRow label="Solicitation ID" value={contract.solicitation_id} label2="Record Type" value2={contract.record_type ?? 'N/A'} />}
                  {!contract.solicitation_id && <DetailRow label="Record Type" value={contract.record_type ?? 'N/A'} />}

                  {/* Dates */}
                  <tr><td colSpan={2} style={{ padding: '4px 0 2px', borderBottom: '1px solid #ccc' }}><strong className="results_title_text" style={{ color: '#0558a5' }}>Dates</strong></td></tr>
                  <DetailRow label="Date Signed" value={formatDate(contract.signed_date)} label2="Effective Date" value2={formatDate(contract.effective_date)} />
                  <DetailRow label="Last Modified (FPDS)" value={formatDateTime(contract.atom_feed_modified_date)} label2="Created Date" value2={formatDate(contract.created_date)} />
                  <DetailRow label="Current Completion Date" value={formatDate(contract.current_completion_date)} label2="Ultimate Completion Date" value2={formatDate(contract.ultimate_completion_date)} />

                  {/* Dollar Values */}
                  <tr><td colSpan={2} style={{ padding: '4px 0 2px', borderBottom: '1px solid #ccc' }}><strong className="results_title_text" style={{ color: '#0558a5' }}>Dollar Values</strong></td></tr>
                  <DetailRow label="Action Obligation" value={formatCurrency(contract.obligated_amount)} label2="Base and All Options Value" value2={formatCurrency(contract.base_and_all_options_value)} />
                  <DetailRow label="Base and Exercised Options" value={formatCurrency(contract.base_and_exercised_options_value)} label2="Total Obligated Amount" value2={formatCurrency(contract.total_obligated_amount)} />

                  {/* Contracting Details */}
                  <tr><td colSpan={2} style={{ padding: '4px 0 2px', borderBottom: '1px solid #ccc' }}><strong className="results_title_text" style={{ color: '#0558a5' }}>Contracting Details</strong></td></tr>
                  <DetailRow label="Contracting Agency" value={`${contract.agency_name ?? 'N/A'} (${contract.agency_code ?? ''})`} label2="Funding Agency" value2={contract.funding_agency_name ?? 'N/A'} />
                  <DetailRow label="Contracting Office" value={contract.contracting_office_name ?? 'N/A'} label2="Funding Office" value2={contract.funding_office_name ?? 'N/A'} />
                  <DetailRow label="Action Type" value={contract.action_type_description ?? 'N/A'} label2="Pricing Type" value2={contract.pricing_type_description ?? 'N/A'} />
                  {contract.reason_for_modification && <DetailRow label="Reason for Modification" value={contract.reason_for_modification} />}

                  {/* Vendor */}
                  <tr><td colSpan={2} style={{ padding: '4px 0 2px', borderBottom: '1px solid #ccc' }}><strong className="results_title_text" style={{ color: '#0558a5' }}>Vendor</strong></td></tr>
                  <DetailRow label="Vendor Name" value={contract.vendor_name ?? 'N/A'} label2="Unique Entity ID (UEI)" value2={contract.uei_sam ?? 'N/A'} />
                  {contract.country_of_origin && <DetailRow label="Country of Origin" value={contract.country_of_origin} />}

                  {/* Classification */}
                  <tr><td colSpan={2} style={{ padding: '4px 0 2px', borderBottom: '1px solid #ccc' }}><strong className="results_title_text" style={{ color: '#0558a5' }}>Product / Service Classification</strong></td></tr>
                  <DetailRow
                    label="PSC"
                    value={`${contract.psc_code ?? 'N/A'}${contract.psc_description ? ` — ${contract.psc_description}` : ''}`}
                    label2="NAICS"
                    value2={`${contract.naics_code ?? 'N/A'}${contract.naics_description ? ` — ${contract.naics_description}` : ''}`}
                  />

                  {/* Competition */}
                  <tr><td colSpan={2} style={{ padding: '4px 0 2px', borderBottom: '1px solid #ccc' }}><strong className="results_title_text" style={{ color: '#0558a5' }}>Competition</strong></td></tr>
                  <DetailRow
                    label="Extent Competed"
                    value={contract.extent_competed ? `${contract.extent_competed}${EXTENT_COMPETED_LABELS[contract.extent_competed] ? ` - ${EXTENT_COMPETED_LABELS[contract.extent_competed]}` : ''}` : 'N/A'}
                    label2="Set-Aside Type"
                    value2={contract.type_of_set_aside ?? 'N/A'}
                  />
                  <DetailRow
                    label="Number of Offers"
                    value={contract.number_of_offers_received?.toString() ?? 'N/A'}
                    label2="Fair Opportunity"
                    value2={contract.fair_opportunity_limited_sources ?? 'N/A'}
                  />
                  {contract.reason_not_competed && <DetailRow label="Reason Not Competed" value={contract.reason_not_competed} />}
                  {contract.commercial_item_acquisition_procedures && <DetailRow label="Commercial Item Procedures" value={contract.commercial_item_acquisition_procedures} />}

                  {/* Place of Performance */}
                  <tr><td colSpan={2} style={{ padding: '4px 0 2px', borderBottom: '1px solid #ccc' }}><strong className="results_title_text" style={{ color: '#0558a5' }}>Place of Performance</strong></td></tr>
                  <DetailRow
                    label="Address"
                    value={[contract.pop_street_address, contract.pop_city, contract.pop_state_code, contract.pop_zip_code, contract.pop_country_code].filter(Boolean).join(', ') || 'N/A'}
                    label2="Congressional District"
                    value2={contract.pop_congressional_district ?? 'N/A'}
                  />

                  {/* Contract Attributes */}
                  <tr><td colSpan={2} style={{ padding: '4px 0 2px', borderBottom: '1px solid #ccc' }}><strong className="results_title_text" style={{ color: '#0558a5' }}>Contract Attributes</strong></td></tr>
                  <DetailRow label="Contract Financing" value={contract.contract_financing ?? 'N/A'} label2="Cost or Pricing Data" value2={contract.cost_or_pricing_data ?? 'N/A'} />
                  <DetailRow label="Performance-Based" value={contract.performance_based_service_contract ?? 'N/A'} label2="Multi-Year Contract" value2={contract.multi_year_contract ?? 'N/A'} />
                  <DetailRow label="Consolidated Contract" value={contract.consolidated_contract ?? 'N/A'} label2="National Interest Action" value2={contract.national_interest_action_code ?? 'N/A'} />

                  {/* Description */}
                  {contract.description_of_requirement && (
                    <>
                      <tr><td colSpan={2} style={{ padding: '4px 0 2px', borderBottom: '1px solid #ccc' }}><strong className="results_title_text" style={{ color: '#0558a5' }}>Description</strong></td></tr>
                      <tr className="result-detail-row">
                        <td colSpan={2}>
                          <span className="results_text">{contract.description_of_requirement}</span>
                        </td>
                      </tr>
                    </>
                  )}
                </tbody>
              </table>
            </td>
          </tr>
        )}
      </tbody>
    </table>
  )
}

function DetailRow({ label, value, label2, value2 }: { label: string; value: string; label2?: string; value2?: string }) {
  return (
    <tr className="result-detail-row">
      <td width="50%">
        <span className="results_title_text">{label}: </span>
        <span className="results_text">{value}</span>
      </td>
      {label2 ? (
        <td width="50%">
          <span className="results_title_text">{label2}: </span>
          <span className="results_text">{value2}</span>
        </td>
      ) : (
        <td width="50%"></td>
      )}
    </tr>
  )
}

function PaginationLinks({ currentPage, totalPages, goToPage }: { currentPage: number; totalPages: number; goToPage: (p: number) => void }) {
  const pages = generatePageNumbers(currentPage, totalPages)
  return (
    <span>
      {currentPage > 1 && (
        <a href="#" onClick={(e) => { e.preventDefault(); goToPage(currentPage - 1) }} className="link_text">
          {'<< Prev'}
        </a>
      )}
      {pages.map((p, i) =>
        p === '...' ? (
          <span key={`e${i}`} className="results_text"> ... </span>
        ) : p === currentPage ? (
          <span key={p} className="page-current results_title_text"> [{p}] </span>
        ) : (
          <a key={p} href="#" onClick={(e) => { e.preventDefault(); goToPage(p as number) }} className="link_text"> {p} </a>
        )
      )}
      {currentPage < totalPages && (
        <a href="#" onClick={(e) => { e.preventDefault(); goToPage(currentPage + 1) }} className="link_text">
          {'Next >>'}
        </a>
      )}
    </span>
  )
}

function generatePageNumbers(current: number, total: number): (number | string)[] {
  if (total <= 10) return Array.from({ length: total }, (_, i) => i + 1)
  const pages: (number | string)[] = [1]
  if (current > 4) pages.push('...')
  for (let i = Math.max(2, current - 2); i <= Math.min(total - 1, current + 2); i++) {
    pages.push(i)
  }
  if (current < total - 3) pages.push('...')
  pages.push(total)
  return pages
}
