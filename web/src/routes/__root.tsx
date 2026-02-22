/// <reference types="vite/client" />
import {
  Outlet,
  createRootRoute,
  HeadContent,
  Scripts,
  ErrorComponent,
} from '@tanstack/react-router'
import type { ErrorComponentProps, ReactNode } from 'react'
import appCss from '~/styles.css?url'

export const Route = createRootRoute({
  head: () => ({
    meta: [
      { charSet: 'utf-8' },
      { name: 'viewport', content: 'width=device-width, initial-scale=1' },
      { title: 'FPDS-NG ezSearch' },
    ],
    links: [
      { rel: 'stylesheet', href: appCss },
    ],
  }),
  component: RootComponent,
  shellComponent: RootDocument,
  errorComponent: (props: ErrorComponentProps) => (
    <RootDocument>
      <div style={{ padding: 20, textAlign: 'center' }}>
        <h2 style={{ color: '#d00' }}>Search Application Error</h2>
        <p style={{ color: '#444' }}>{props.error?.message || 'An unexpected error occurred.'}</p>
        <button
          onClick={() => props.reset()}
          style={{ padding: '8px 16px', background: '#0558a5', color: '#fff', border: 'none', cursor: 'pointer' }}
        >
          Try Again
        </button>
      </div>
    </RootDocument>
  ),
  notFoundComponent: () => (
    <RootDocument>
      <div style={{ padding: 20, textAlign: 'center' }}>
        <h2 style={{ color: '#444' }}>404 - Not Found</h2>
        <p>The page you are looking for does not exist.</p>
        <a href="/" style={{ color: '#0558a5', textDecoration: 'underline' }}>Return to Search</a>
      </div>
    </RootDocument>
  ),
})

function RootDocument({ children }: Readonly<{ children: ReactNode }>) {
  return (
    <html lang="en">
      <head>
        <HeadContent />
      </head>
      <body>
        {children}
        <Scripts />
      </body>
    </html>
  )
}

function RootComponent() {
  return (
    <div className="fpds-page">
      <div className="fpds-topbar">
        <table className="fpds-header-table">
          <tbody>
            <tr>
              <td className="fpds-header-logo">
                <a href="/">
                  <span className="fpds-logo-text">FPDS</span>
                </a>
              </td>
              <td className="fpds-header-title">
                <a href="/" className="fpds-title-link">
                  Federal Procurement Data System — Contract Award Search
                </a>
              </td>
              <td className="fpds-header-right">
                <span className="fpds-subtitle">Data sourced from FPDS.gov Atom Feed</span>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
      <Outlet />
      <div className="fpds-footer">
        <table width="100%">
          <tbody>
            <tr>
              <td align="center">
                <span className="help_text">
                  This is not an official U.S. Government website. Data sourced from the FPDS.gov Atom Feed.
                </span>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  )
}
