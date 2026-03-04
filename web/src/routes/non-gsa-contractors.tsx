import { createFileRoute } from '@tanstack/react-router'
import { useState } from 'react'
import { getNonGsaContractors } from '~/lib/search'

export const Route = createFileRoute('/non-gsa-contractors')({
  validateSearch: (search: Record<string, unknown>) => ({
    page: search.page ? Number(search.page) : 1,
    limit: search.limit ? Number(search.limit) : 50,
  }),
  loaderDeps: ({ search }) => search,
  loader: async ({ deps }) => {
    return getNonGsaContractors({ data: deps })
  },
  component: NonGsaContractorsPage,
})

function NonGsaContractorsPage() {
  const { results, page, limit } = Route.useLoaderData()
  const search = Route.useSearch()
  const navigate = Route.useNavigate()

  const [columns, setColumns] = useState({
    street_address: false,
    city: false,
    state: false,
    zip_code: false,
    is_small_business: false,
    is_women_owned: false,
    is_veteran_owned: false,
  })

  const handleColumnToggle = (col: keyof typeof columns) => {
    setColumns(prev => ({ ...prev, [col]: !prev[col] }))
  }

  const activeColCount = 4 + Object.values(columns).filter(Boolean).length

  const handleNextPage = () => {
    navigate({
      search: { ...search, page: page + 1 },
    })
  }

  const handlePrevPage = () => {
    if (page > 1) {
      navigate({
        search: { ...search, page: page - 1 },
      })
    }
  }

  return (
    <div className="fpds-main-content">
      <div style={{ padding: 20 }}>
        <h2>Non-eLibrary Contractors with FPDS Sales</h2>
        <p>This list shows contractors that have recorded FPDS contract actions with phone numbers but are not present in the GSA eLibrary.</p>
        
        <div style={{ marginBottom: '1rem' }}>
          <button 
            onClick={() => navigate({ to: '/' })} 
            style={{ padding: '6px 12px', background: '#0558a5', color: '#fff', border: 'none', borderRadius: '4px', cursor: 'pointer', marginRight: '10px' }}
          >
            &larr; Back to Search
          </button>
        </div>

        <div style={{ marginBottom: '1rem', padding: '15px', background: '#f5f5f5', borderRadius: '4px', border: '1px solid #ddd' }}>
          <strong style={{ display: 'block', marginBottom: '10px' }}>Optional Columns:</strong>
          <div style={{ display: 'flex', gap: '15px', flexWrap: 'wrap' }}>
            {Object.keys(columns).map(col => (
              <label key={col} style={{ cursor: 'pointer', display: 'flex', alignItems: 'center' }}>
                <input
                  type="checkbox"
                  checked={columns[col as keyof typeof columns]}
                  onChange={() => handleColumnToggle(col as keyof typeof columns)}
                  style={{ marginRight: '6px' }}
                />
                {col.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}
              </label>
            ))}
          </div>
        </div>

        <table className="fpds-table" style={{ width: '100%', marginTop: '20px' }}>
          <thead>
            <tr>
              <th className="fpds-table-header">Vendor Name</th>
              <th className="fpds-table-header">UEI (SAM)</th>
              <th className="fpds-table-header">Phone Number</th>
              <th className="fpds-table-header">Email</th>
              {columns.street_address && <th className="fpds-table-header">Street Address</th>}
              {columns.city && <th className="fpds-table-header">City</th>}
              {columns.state && <th className="fpds-table-header">State</th>}
              {columns.zip_code && <th className="fpds-table-header">Zip Code</th>}
              {columns.is_small_business && <th className="fpds-table-header">Small Business</th>}
              {columns.is_women_owned && <th className="fpds-table-header">Women Owned</th>}
              {columns.is_veteran_owned && <th className="fpds-table-header">Veteran Owned</th>}
            </tr>
          </thead>
          <tbody>
            {results.length === 0 ? (
              <tr>
                <td colSpan={activeColCount} style={{ textAlign: 'center', padding: '20px' }}>No contractors found.</td>
              </tr>
            ) : (
              results.map((contractor, idx) => (
                <tr key={`${contractor.uei_sam}-${idx}`} className="fpds-table-row">
                  <td className="fpds-table-cell">{contractor.vendor_name || 'N/A'}</td>
                  <td className="fpds-table-cell">{contractor.uei_sam || 'N/A'}</td>
                  <td className="fpds-table-cell">{contractor.phone_no || 'N/A'}</td>
                  <td className="fpds-table-cell">
                    {contractor.email ? (
                      <a href={`mailto:${contractor.email}`}>{contractor.email}</a>
                    ) : (
                      'N/A'
                    )}
                  </td>
                  {columns.street_address && <td className="fpds-table-cell">{contractor.street_address || 'N/A'}</td>}
                  {columns.city && <td className="fpds-table-cell">{contractor.city || 'N/A'}</td>}
                  {columns.state && <td className="fpds-table-cell">{contractor.state || 'N/A'}</td>}
                  {columns.zip_code && <td className="fpds-table-cell">{contractor.zip_code || 'N/A'}</td>}
                  {columns.is_small_business && <td className="fpds-table-cell">{contractor.is_small_business ? 'Yes' : 'No'}</td>}
                  {columns.is_women_owned && <td className="fpds-table-cell">{contractor.is_women_owned ? 'Yes' : 'No'}</td>}
                  {columns.is_veteran_owned && <td className="fpds-table-cell">{contractor.is_veteran_owned ? 'Yes' : 'No'}</td>}
                </tr>
              ))
            )}
          </tbody>
        </table>

        <div style={{ marginTop: '20px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <div>
            <button 
              onClick={handlePrevPage} 
              disabled={page <= 1}
              style={{ padding: '6px 12px', background: page <= 1 ? '#ccc' : '#0558a5', color: '#fff', border: 'none', borderRadius: '4px', cursor: page <= 1 ? 'not-allowed' : 'pointer' }}
            >
              Previous
            </button>
            <span style={{ margin: '0 15px' }}>Page {page}</span>
            <button 
              onClick={handleNextPage}
              disabled={results.length < limit}
              style={{ padding: '6px 12px', background: results.length < limit ? '#ccc' : '#0558a5', color: '#fff', border: 'none', borderRadius: '4px', cursor: results.length < limit ? 'not-allowed' : 'pointer' }}
            >
              Next
            </button>
          </div>
          <div>
            Showing {results.length} contractors
          </div>
        </div>
      </div>
    </div>
  )
}
