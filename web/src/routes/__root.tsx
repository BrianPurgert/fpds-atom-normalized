/// <reference types="vite/client" />
import {
  Outlet,
  createRootRoute,
  HeadContent,
  Scripts,
} from '@tanstack/react-router'
import type { ReactNode } from 'react'
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
