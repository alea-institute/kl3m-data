# KL3M Data Sources

This document provides an overview of all data sources in the KL3M dataset and their implementation details.

## Overview

The KL3M dataset integrates legal and governmental text from various jurisdictions and sources. Each source is implemented as a Python class that extends the `BaseSource` abstract class.

All sources share a common interface for downloading and processing documents:
- `download_id()` - Download a specific document by ID
- `download_date()` - Download documents from a specific date
- `download_date_range()` - Download documents from a date range
- `download_all()` - Download all available documents

## Core Structure

### Base Classes

1. **BaseSource** (`kl3m_data/sources/base_source.py`)
   - Abstract base class with HTTP utilities and standard interface methods
   - Handles authentication, rate limiting, and error handling

2. **Document** (`kl3m_data/sources/base_document.py`)
   - Extends Dublin Core metadata standard for document representation
   - Provides S3 storage functionality

## Data Sources by Jurisdiction

### United States Sources

#### 1. Court of Appeals (CAP)
- **Dataset ID**: `cap`
- **Description**: U.S. Courts of Appeals decisions and other U.S. case law
- **Implementation**: `kl3m_data/sources/us/cap/cap_source.py`
- **License**: CC0 1.0 Universal (Public Domain)
- **Origin**: Harvard Law School Library's Innovation Lab and Ravel Law, with later data augmentation by Fastcase
- **Statistics**:
  - Documents: 6,919,296
  - Total Tokens: 16.7B
  - Documents > 8K Tokens: 5.12%
  - Documents > 32K Tokens: 0.12%
- **Format**: Original HTML converted to Markdown
- **Features**:
  - Comprehensive collection of U.S. case law from 1658 through 2020
  - Includes decisions from federal courts, state appellate courts, and territorial courts
  - Contains machine-readable text from over 40 million pages of court decisions

#### 2. Dockets
- **Dataset ID**: `dockets`
- **Description**: U.S. federal court dockets
- **Implementation**: `kl3m_data/sources/us/dockets/dockets_source.py`

#### 3. .Gov Websites
- **Dataset ID**: `dotgov`
- **Description**: U.S. federal government website content from .gov, .mil, and other government domains
- **Implementation**: `kl3m_data/sources/us/dotgov/dotgov_source.py`
- **Structure**: Organized by domain (e.g., irs.gov, epa.gov)
- **License**: Public domain under 17 U.S.C. § 105 (filtered for government content)
- **Statistics**:
  - Documents: 3,233,136
  - Total Tokens: 36.6B
  - Documents > 8K Tokens: 14.49%
  - Documents > 32K Tokens: 4.00%
- **Content Types**: Various formats including HTML, PDF, Office documents, XML, and plain text
- **Final Formats**: Markdown, Text, XML, JSON, YAML
- **Features**:
  - Comprehensive collection of federal government information across all branches
  - Contains policies, reports, guidance documents, and public-facing resources
  - Content filtered using allowlists and blocklists to ensure relevance
  - Each document assigned a blake2b cryptographic hash for integrity verification

#### 4. Electronic Code of Federal Regulations (ECFR)
- **Dataset ID**: `ecfr`
- **Description**: Code of Federal Regulations in electronic format
- **Implementation**: `kl3m_data/sources/us/ecfr/ecfr_source.py`
- **License**: Public domain under 17 U.S.C. § 105
- **Origin**: U.S. Government Publishing Office (GPO)
- **Statistics**:
  - Documents: 262,243
  - Total Tokens: 137.3M
  - Documents > 8K Tokens: 0.53%
  - Documents > 32K Tokens: 0.04%
- **Format**: Original HTML converted to Markdown
- **Features**:
  - Complete body of current federal regulations organized into 50 titles
  - Content retrieved through the official eCFR API
  - Updates daily with the latest regulatory changes
  - Preserves hierarchical structure of titles, chapters, subchapters, parts, subparts, and sections
  - Includes comprehensive metadata and cryptographic hashing for integrity verification

#### 5. SEC EDGAR Filings
- **Dataset ID**: `edgar`
- **Description**: Securities and Exchange Commission company filings
- **Implementation**: `kl3m_data/sources/us/edgar/edgar_source.py`
- **License**: Generally available for free use under securities laws (Securities Act of 1933, Securities Exchange Act of 1934, etc.)
- **Origin**: U.S. Securities and Exchange Commission (SEC)
- **Statistics**:
  - Documents (Initial Collection): 74,063,501
  - Representations (Processed): 30,474,244
  - Parquet (Final Format): 44,768,118
- **Features**:
  - Comprehensive repository of public company disclosures dating back to 1996
  - Includes annual reports (10-K), quarterly reports (10-Q), registration statements, proxy statements, and more
  - Documents extracted from daily feed files using sophisticated parsing techniques
  - Handles UUEncoded content through automatic decoding
  - Each document includes extensive metadata:
    - Unique ID in the format `cik/accession_number/sequence`
    - URL to the document on the SEC website
    - MIME type based on file extension
    - Complete submission and document metadata
  - Supports document type filtering and date ranges
  - Uses daily feed files to efficiently process filings

#### 6. Federal Depository Library Program (FDLP)
- **Dataset ID**: `fdlp`
- **Description**: Documents from the FDLP Electronic Collection Archive
- **Implementation**: `kl3m_data/sources/us/fdlp/fdlp_source.py`
- **License**: Public domain under 44 USC 1911
- **Origin**: U.S. Government Publishing Office (GPO)
- **Statistics**:
  - Documents: 454,290
  - Total Tokens: 14.3B
  - Documents > 8K Tokens: 50.81%
  - Documents > 32K Tokens: 23.25%
- **Format**: Plain Text, Markdown, XML, JSON, YAML
- **Features**:
  - Comprehensive repository of U.S. Government publications
  - Contains notably longer documents than many other datasets
  - Retrieves documents via Permanent Link Method or PURL Redirection Method
  - Extracts metadata from the Catalog of U.S. Government Publications (CGP)
  - Supports ID ranges with both "lps" and "gpo" prefixes
  - Provides permanent public access to official government information

#### 7. Federal Register (FR)
- **Dataset ID**: `fr`
- **Description**: Daily journal of U.S. federal government activities
- **Implementation**: `kl3m_data/sources/us/fr/fr_source.py`
- **License**: Public domain under 17 U.S.C. § 105
- **Origin**: Government Publishing Office (GPO) and National Archives and Records Administration (NARA)
- **Statistics**:
  - Documents (Initial Collection): 3,396,818
  - Representations (Processed): 3,396,455
  - Parquet (Final Format): 3,396,389
  - Total Tokens: 21.2B
  - Documents > 8K Tokens: 7.19%
  - Documents > 32K Tokens: 1.35%
- **Format**: Markdown, Plain Text, XML, JSON, YAML
- **Features**:
  - Complete collection of Federal Register documents since 1995
  - Includes federal agency regulations, proposed rules, public notices, executive orders, and more
  - Retrieves multiple formats (XML, plain text, PDF, HTML) for each document
  - Employs date-based retrieval approach using the official Federal Register API
  - Transforms XML documents using XSLT stylesheet to preserve document structure
  - Essential resource for legal research, regulatory compliance, and policy analysis

#### 8. GovInfo
- **Dataset ID**: `govinfo`
- **Description**: U.S. Government Publishing Office documents from all three branches of the Federal Government
- **Implementation**: `kl3m_data/sources/us/govinfo/govinfo_source.py`
- **License**: Public domain under 17 U.S.C. § 105
- **Origin**: United States Government Publishing Office (GPO)
- **Statistics**:
  - Documents (Initial Collection): 14,655,232
  - Representations (Processed): 13,353,022
  - Parquet (Final Format): 11,144,653
- **Collections**:
  - Congressional materials (BILLS, CHRG, CREC, CRPT)
  - Presidential documents (COMPS, PPP)
  - Court opinions (USCOURTS)
  - Regulatory materials (LSA)
  - Government publications (GPO, GOVPUB, GAOREPORTS)
  - Many other specialized collections
- **Features**:
  - Hierarchical organization with packages, granules, and collections
  - Multiple content formats (PDF, text, HTML/XML)
  - Rich metadata including package/granule IDs, collection codes, dates, and authors
  - Documents may appear in multiple collections (indicated by semicolon-delimited collection IDs)
  - Content retrieval prioritizes text/PDF formats for processing efficiency

#### 9. RECAP Archive
- **Dataset ID**: `recap`
- **Description**: Public court records from the PACER system, collected by the Free Law Project's RECAP initiative
- **Implementation**: `kl3m_data/sources/us/recap/recap_source.py`
- **License**: Public domain under 17 U.S.C. § 105
- **Origin**: Free Law Project's Court Listener platform
- **Statistics**:
  - Documents (Initial Collection): 16,762,471
  - Representations (Processed): 14,423,347
  - Parquet (Final Format): 14,265,800
  - Total Tokens: 67.8B
  - Documents > 8K Tokens: 11.21%
  - Documents > 32K Tokens: 1.23%
- **Format**: Plain Text, Markdown, XML, JSON, YAML
- **Features**:
  - Comprehensive collection of federal court documents including complaints, motions, orders, and opinions
  - Two primary document types: PDF court filings and XML docket files
  - Coverage across all federal court types (District, Bankruptcy, Appellate, and Specialized Courts)
  - Documents accessed from Court Listener S3 bucket
  - Content includes PDF documents and XML docket files
  - Extracts additional metadata using pypdfium2 for PDF documents
  - Maps court identifiers to full court names

#### 10. RECAP Documents
- **Dataset ID**: `recap_docs`
- **Description**: Document attachments from the PACER system collected by the Free Law Project's RECAP initiative
- **Implementation**: `kl3m_data/sources/us/recap_docs/recap_docs_source.py`
- **License**: Public domain under 17 U.S.C. § 105 and CC0/Public Domain designation
- **Origin**: Free Law Project's Court Listener platform
- **Statistics**:
  - Documents (Initial Collection): 1,863,733
  - Representations (Processed): 1,691,658
  - Parquet (Final Format): 1,691,655
  - Total Tokens: 5.4B
  - Documents > 8K Tokens: 11.34%
  - Documents > 32K Tokens: 0.38%
- **Format**: Plain Text, Markdown (converted from various original formats)
- **Features**:
  - Specifically contains file attachments in various formats (unlike the main RECAP Archive)
  - Includes legal briefs, expert witness reports, financial documents, contracts, and correspondence
  - Accesses documents from Court Listener S3 bucket with different prefix directories for file formats
  - Converts various file formats (Word, PDF, audio transcripts, WordPerfect) into text representations
  - Complements the main RECAP Archive by providing the attachment files referenced in court filings

#### 11. Regulations.gov Documents
- **Dataset ID**: `reg_docs`
- **Description**: Materials submitted to the federal rulemaking portal, including public comments, proposed rules, and supporting documents
- **Implementation**: `kl3m_data/sources/us/reg_docs/reg_docs_source.py`
- **License**: Public domain under 17 U.S.C. § 105
- **Origin**: Regulations.gov, operated by the eRulemaking Program Management Office within the GSA
- **Statistics**:
  - Documents (Initial Collection): 1,279,349
  - Representations (Processed): 811,163
  - Parquet (Final Format): 785,062
  - Total Tokens: 10.7B
  - Documents > 8K Tokens: 18.35%
  - Documents > 32K Tokens: 5.71%
- **Format**: Markdown, Plain Text
- **Features**:
  - Captures the participatory aspects of the federal regulatory process
  - Covers diverse domains (environmental protection, healthcare, financial services, etc.)
  - Particularly long and detailed documents (mean of 10,354.1 tokens per document)
  - Collection from official Regulations.gov API with date-based retrieval
  - Diverse document types including public comments, supporting materials, agency guidance
  - Complements Federal Register and eCFR datasets by providing insight into regulation development
  - Implements adaptive rate limiting based on API response headers

#### 12. United States Code (USC)
- **Dataset ID**: `usc`
- **Description**: Official compilation of U.S. federal laws
- **Implementation**: `kl3m_data/sources/us/usc/usc_source.py`
- **License**: Public domain under 17 U.S.C. § 105
- **Origin**: Office of the Law Revision Counsel of the U.S. House of Representatives
- **Statistics**:
  - Documents: 69,391
  - Total Tokens: 69.2M
  - Documents > 8K Tokens: 1.51%
  - Documents > 32K Tokens: 0.13%
  - Mean Tokens per Document: 997.9
  - Median Tokens per Document: 360
- **Format**: Original XHTML converted to Markdown
- **Features**:
  - Organizes all general and permanent laws into 54 titles by subject matter
  - Covers Constitutional law, Government Organization, Economic Regulation, Judiciary, and more
  - Downloads ZIP archives from the U.S. House of Representatives website
  - Extracts document fragments using HTML comment markers
  - Preserves rich metadata including document ID, citation information, and hierarchical position
  - Serves as a primary reference for federal statutory law
  - Complements other federal legal materials (eCFR, Federal Register) in the collection

#### 13. USPTO Granted Patents
- **Dataset ID**: `uspto`
- **Description**: Patents issued by the United States Patent and Trademark Office (USPTO)
- **Implementation**: `kl3m_data/sources/us/uspto_patents/uspto_patents_source.py`
- **License**: Public domain per 37 CFR 1.71 et seq.
- **Origin**: United States Patent and Trademark Office, U.S. Department of Commerce
- **Statistics**:
  - Documents (Initial Collection): 6,586,666
  - Representations (Processed): 6,413,833
  - Parquet (Final Format): 6,413,827
  - Total Tokens: 81.6B
  - Documents > 8K Tokens: 56.34%
  - Documents > 32K Tokens: 5.18%
  - Mean Document Length: 12,718.7 tokens
- **Format**: Original Text/XML converted to Markdown
- **Features**:
  - More than 6.4 million patents spanning all fields of technology
  - One of the largest repositories of specialized technical and scientific language
  - Covers mechanical engineering, electrical engineering, chemistry, biotechnology, software, and design patents
  - Downloads from the official USPTO bulk data repository
  - Parses older patents in text format using segment markers (TTL, ABST, BSPR, CLPR)
  - Processes newer patents in XML format using the ICE XML schema
  - Standardizes both formats into consistent markdown structure
  - Preserves rich metadata including patent number, filing date, inventor names, and classification codes

### United Kingdom Sources

#### 1. UK Legislation
- **Dataset ID**: `ukleg`
- **Description**: Primary and secondary legislation from the United Kingdom
- **Implementation**: `kl3m_data/sources/uk/uk_legislation/uk_legislation_source.py`
- **License**: Open Government Licence v3.0 (compatible with CC-BY)
- **Origin**: The National Archives of the UK Government via legislation.gov.uk
- **Statistics**:
  - Documents: 219,190
  - Total Tokens: 7.2B
  - Documents > 8K Tokens: 36.06%
  - Documents > 32K Tokens: 21.09%
  - Mean Document Length: 32,675.6 tokens
  - Median Document Length: 2,493 tokens
- **Format**: Original HTML converted to Markdown
- **Features**:
  - Spans over 750 years of British legal history (1267 to present)
  - Includes Acts of Parliament, Statutory Instruments, Statutory Rules and Orders, UK Church Instruments, and UK Local Acts
  - Downloaded from a bulk data package from the UK National Archives
  - Preserves hierarchical structure of legislation (parts, chapters, sections, subsections, paragraphs)
  - Maintains embedded metadata (title, year, type, number, enactment date)
  - Captures cross-references, legal formatting conventions, and annotations
  - Contains notably long documents (the longest containing 1.4 million tokens)

### European Union Sources

#### 1. EU Official Journal
- **Dataset ID**: `eu_oj`
- **Description**: Official Journal of the European Union
- **Implementation**: `kl3m_data/sources/eu/eu_oj/eu_oj.py`
- **License**: Text available for free use under 2011/833/EU; metadata dedicated to public domain under CC0
- **Origin**: Publications Office of the European Union
- **Statistics**:
  - Documents (Initial Collection): 1,389,632
  - Representations (Processed): 1,225,498
  - Parquet (Final Format): 120,573
- **Structure**: Data organized from the EU Publications Office SPARQL endpoint
- **Features**:
  - Collects documents from the official EU Publications Office
  - Supports filtering by year and language (default range from 2004 to present)
  - Retrieves FMX4 format documents via URL links extracted from RDF metadata
  - Multi-language support with primary focus on English, Spanish, French, and German
  - Comprehensive EU language coverage (23 languages available)
  - Uses SPARQL queries to retrieve document metadata
  - Documents organized by year and language
  - Extracts metadata including title, authors, publication date
  - Assigns unique identifiers based on EU Publication Office URIs

## Pipeline Processing

All sources follow the S3 pipeline process:

1. **Download**: Source-specific classes retrieve documents from their origins
2. **Documents Stage**: Raw documents are stored in S3 with Dublin Core metadata
3. **Representations Stage**: Documents are processed, tokenized, and stored
4. **Parquet Stage**: Binary format optimized for ML training

## Configuration and Customization

Each source can be configured with parameters:
- Date ranges for time-specific sources
- Rate limiting parameters
- Language preferences where applicable
- Document type filtering

For example:
```python
# Create an EDGAR source for a specific date range
edgar_source = EDGARSource(
    min_date="2020-01-01",
    max_date="2020-12-31",
    delay=1  # 1 second between requests
)

# Download all documents in the date range
for progress in edgar_source.download_date_range(
    start_date=datetime.date(2020, 1, 1),
    end_date=datetime.date(2020, 12, 31)
):
    print(f"Progress: {progress.current}/{progress.total}")
```