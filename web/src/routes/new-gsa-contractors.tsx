import { createFileRoute } from '@tanstack/react-router'
import { getNewGsaContractors } from '~/lib/search'

export const Route = createFileRoute('/new-gsa-contractors')({
  validateSearch: (search: Record<string, unknown>) => ({
    page: search.page ? Number(search.page) : 1,
    limit: search.limit ? Number(search.limit) : 50,
  }),
  loaderDeps: ({ search }) => search,
  loader: async ({ deps }) => {
    return getNewGsaContractors({ data: deps })
  },
  component: NewGsaContractorsPage,
})

function formatDate(dateStr: string | null | undefined): string {
  if (!dateStr) return 'N/A'
  try {
    return new Date(dateStr).toLocaleDateString('en-US', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    })
  } catch {
    return dateStr
  }
}

function NewGsaContractorsPage() {
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
        <h2>New GSA Contractors</h2>
        <p>
          This list shows GSA eLibrary contractors with a calculated contract start date derived from the
          ultimate contract end date minus 20 years. Results exclude null start dates and are sorted newest first.
        </p>

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
              <th className="fpds-table-header">Contract Start Date</th>
              <th className="fpds-table-header">Vendor Name</th>
              <th className="fpds-table-header">Contract Number</th>
              <th className="fpds-table-header">UEI</th>
              <th className="fpds-table-header">Source</th>
              <th className="fpds-table-header">Current Option End</th>
              <th className="fpds-table-header">Ultimate Contract End</th>
              <th className="fpds-table-header">Phone Number</th>
              <th className="fpds-table-header">Email</th>
              <th className="fpds-table-header">Email Domain</th>
              <th className="fpds-table-header">Website Domain</th>
            </tr>
          </thead>
          <tbody>
            {results.length === 0 ? (
              <tr>
                <td colSpan={11} style={{ textAlign: 'center', padding: '20px' }}>No contractors found.</td>
              </tr>
            ) : (
              results.map((contractor) => (
                <tr key={contractor.record_key} className="fpds-table-row">
                  <td className="fpds-table-cell">{formatDate(contractor.contract_start_date)}</td>
                  <td className="fpds-table-cell">{contractor.vendor_name || 'N/A'}</td>
                  <td className="fpds-table-cell">{contractor.contract_number || 'N/A'}</td>
                  <td className="fpds-table-cell">{contractor.uei || 'N/A'}</td>
                  <td className="fpds-table-cell">{contractor.source || 'N/A'}</td>
                  <td className="fpds-table-cell">{formatDate(contractor.current_option_period_end_date)}</td>
                  <td className="fpds-table-cell">{formatDate(contractor.ultimate_contract_end_date)}</td>
                  <td className="fpds-table-cell">{contractor.phone_number || 'N/A'}</td>
                  <td className="fpds-table-cell">
                    {contractor.contact_email ? (
                      <a href={`mailto:${contractor.contact_email}`}>{contractor.contact_email}</a>
                    ) : (
                      'N/A'
                    )}
                  </td>
                  <td className="fpds-table-cell">{contractor.email_domain || 'N/A'}</td>
                  <td className="fpds-table-cell">{contractor.website_domain || 'N/A'}</td>
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
