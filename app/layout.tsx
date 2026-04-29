import "./globals.css";
import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "APEX Enrichment Drafts",
  description: "Review and validate enrichment drafts",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="fr">
      <body>{children}</body>
    </html>
  );
}
