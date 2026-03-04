import { createFileRoute } from '@tanstack/react-router'
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

        <table className="fpds-table" style={{ width: '100%', marginTop: '20px' }}>
          <thead>
            <tr>
              <th className="fpds-table-header">Vendor Name</th>
              <th className="fpds-table-header">UEI (SAM)</th>
              <th className="fpds-table-header">Phone Number</th>
            </tr>
          </thead>
          <tbody>
            {results.length === 0 ? (
              <tr>
                <td colSpan={3} style={{ textAlign: 'center', padding: '20px' }}>No contractors found.</td>
              </tr>
            ) : (
              results.map((contractor, idx) => (
                <tr key={`${contractor.uei_sam}-${idx}`} className="fpds-table-row">
                  <td className="fpds-table-cell">{contractor.vendor_name || 'N/A'}</td>
                  <td className="fpds-table-cell">{contractor.uei_sam || 'N/A'}</td>
                  <td className="fpds-table-cell">{contractor.phone_no || 'N/A'}</td>
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
