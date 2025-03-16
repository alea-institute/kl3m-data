
Practically all large language models have been pretrained on data that is subject to global uncertainty related to copyright, breach of contract, and privacy.  Regardless of what the reader believes about the legal and ethical questions underlying this uncertainty, there is no denying the existence of the many lawsuits and investigations ongoing in major jurisdictions.

In particular, practically all models are anchored into a foundation of  "fair use" or "fair dealings" related to "publicy-available" data.  This critical concept, loosely supported by precedent and limited statute in a small number of jurisdictions,  is not guaranteed to survive - nor is it universally accepted by many within society that it should.

In light of this reality about extant research, we set out on an alternative research agenda - one that is rooted in legal and ethical practices that are well-accepted and free of doubt.

In this paper, we present the first output of that agenda, the KL3M dataset and related tokenizer, software, and APIs.  These assets, open-sourced and maintained by our 501(c)(3), represent one of the largest collections of unencumbered pretraining and supervised fine-tuning data available.  

(TODO: number of sources, number of tokens, size of raw data, availability)

TODO: distribution of sources and tokens by license (public domain government work, OGL-3.0, EU CC-BY).



## Introduction

There are more than 200,000 datasets available on Hugging Face. W

With over 30 copyright lawsuits against AI companies currently in progress in the US alone, the need for legal precedent on the matter of copyright infringement and model training is clear. The outcome of these cases will likely establish whether the argument for “fair use” in model training is a viable one. 

Our primary contribution to a space already saturated with datasets is the research into and development of a path that is free of reliance on the “fair use” argument that is often relied upon for model training. We established and are publishing the framework for determining  permissibility of content usage that can be employed by anyone who wants to gather or audit training data for model training or fine-tuning. ==In addition, we enhance the data provenance visibility of the KL3M dataset by providing Dublin Core metadata for all data in the dataset.==

The continued development and use of AI systems is predicated on these systems being legal, both in the current environment and in the wake of future regulatory and legislative changes. We believe that the development of datasets and models that are transparent, freely available without legal restrictions, and high quality will enable downstream use that is free of the infringement concerns often present.

  

==“More than 40 countries with over one-third of the world’s population have fair use or fair dealing provisions in their copyright laws.”==

  
**
~~Background on why it's important
~~ ~~- Why yet another dataset
~~ ~~- List of recent litigation, public opinion (surveys)
~ - Our take: if you want AI systems to exist, then it's important to create something that's legal (and can survive future regulatory and legislative hurdles)
 **Our primary contribution: research contributing to developing a path that's free of the "fair use" argument** 
- Fair use background
	- Exists in X jurisdictions, not in Y
	- Acknowledge text and data mining exceptions (but they're usually restricted/limited)

## Sources
==why we included them==
### Securities and Exchange Commission Filings
The U.S. Securities and Exchange Commission (SEC) is an independent agency of the federal government of the United States. As part of its role, the SEC requires public companies and other regulated entities to submit numerous filings and disclosures, which with limited exceptions (such as redacted information) are in the public domain. The KL3M dataset includes the following data subsets from the SEC:
- Public agreements
- Annual Form 10-K filings (domestic and foreign)
- Quarterly Form 10-Q filings (domestic and foreign)
- "Current report" Form 8-K filings (domestic and foreign)
- S-# filings. There are numerous S-# filings related to registration, such as the S-4 Registration of securities issued in business combination transactions. The KL3M dataset includes S-1, S-2, S-3, S-4, S-6, and S-8 filings.
- Form 6K for information furnished by foreign private issuers
- Annual Form 20-F for non-Canadian foreign private issuers
- Annual Form 40-F for Canadian private issuers
- Exhibits and addenda sample from the above filings
### Congressional Documents Collection
The Congressional Documents Collection is a collection of various materials ordered to be printed by both the U.S. House and Senate. It is comprised of House Documents, Senate Documents, and Senate Treaty Documents. The House and Senate Documents contain various kinds of materials ordered to be printed by both chambers of Congress, including executive reports from agencies and departments. The Senate Treaty Documents contain treaty text as it has been submitted by the Senate for presidential ratification.
### Congressional Bills
This component includes legislative proposals within the United States Congress from the House of Representatives and Senate. This includes the entire life cycle of bills, from initial introduction to final published bills. 
### Code of Federal Regulations
The Code of Federal Regulations (CFR) is the annual codification of the general and permanent rules published in the Federal Register by the departments and agencies of the United States Federal Government. The CFR is not continuously updated, but is revised in sections on a quarterly basis.
### Electronic Code of Federal Regulations
The Electronic Code of Federal Regulations (eCFR) is an editorial compilation of CFR material and amendments published in the daily Federal Register. The eCFR is continuously updated, but is not the official legal edition of the CFR.
### Federal Depository Library Program
The Federal Depository Library Program (FDLP) provides a digitized record of the official documents published by the U.S. Government Publishing Office (GPO), GPO’s Superintendent of Documents, and other Federal agency publishers related to the FDLP. 
### Federal Register
The Federal Register is the official daily publication for the following materials of the United States Federal Government: Presidential Documents, Executive Orders, proposed, interim, and final rules and regulations, and notices by Federal Agencies, as well as notices of hearings, decisions, investigations, and committee meetings. The National Archives and Records Administration is responsible for publishing the Federal Register. 
### Federal Judicial Center
The Federal Judicial Center serves as the education and research agency for the U.S. federal courts. The KL3M dataset contains material published by the Federal Judicial Center, including research reports, monographs on substantive legal subjects, manuals, and reference guides.
### CIA World Factbook
The World Factbook, developed by the U.S. Central Intelligence Agency (CIA), provides basic intelligence on the history, people, government, economy, energy, geography, environment, communications, transportation, military, terrorism, and transnational issues for 265 world entities.
### Congressional Research Services
The Congressional Research Service (CRS) is a U.S. federal legislative branch agency that provides research services exclusively to congressional committees and Members of Congress. CRS reports, which are available to the public, cover a broad range of topics and are intended to reflect objective, nonpartisan research and analysis. 
### United States Government Manual
The U.S. Government Manual is a regularly updated special edition of the Federal Register. Its contents include leadership tables and descriptions of agency activities and programs of the executive, judicial, and legislative branches of Federal U.S. Government, as well as those of quasi-official agencies and international organizations in which the United States is a participating member. 
### Library of Congress - Country Profiles
This collection of nearly 50 country profiles of foreign nations provides brief, summarized information on each country’s historical background, geography, society, economy, transportation and telecommunications, government and politics, and national security. This series of profiles is a subset of the Country Studies Program, formerly the Army Area Handbook Program. The collection was written between 2004 and 2008 and has not been updated since then.
### Statutes at Large
The United States Statutes at Large is the permanent collection of all laws and resolutions enacted during each session of Congress. The Statutes at Large is prepared and published by the Office of the Federal Register (OFR). The printed edition of the Statutes at Large is legal evidence of the laws, concurrent resolutions, proclamations by the President, and proposed and ratified amendments to the Constitution.
### Regulatory Submissions
Any member of the public may submit comments on U.S. federal regulation through the website regulations.gov. The KL3M dataset contains the documents submitted as attachments to public comments.
### United States Code
The United States Code is a consolidation and codification by subject matter of the general and permanent federal laws of the United States. It is prepared by the Office of the Law Revision Counsel of the United States House of Representatives.
### ==Court Documents==

#### ==Opinions==
==Source: Court Listener bulk data tarfiles (c. 2022, no longer available)==
#### ==Dockets==
==Source: Court Listener metadata, archive.org docket mirror==
#### ==Other Documents (e.g., motions, orders, evidence)==
==Source: RECAP S3 bucket==
### ==Court Documents - Opinions==
==U.S. federal and state court opinions are written explanations from judges that explain their decision and the facts and legal reasoning supporting it. The KL3M dataset contains court opinions obtained from two sources: PACER and CourtListener.==
#### ==Public Access to Court Electronic Records== 
==Public Access to Court Electronic Records (PACER) is a service of the federal Judiciary that provides access to federal court records.==
#### ==CourtListener==
==CourtListener is a service operated by the non-profit Free Law Project to provide free access to primary source legal materials. The court opinions from CourtListener included in the KL3M dataset contain both federal and state opinions.==
### ==Court Documents - Non-Opinions==
==Non-opinion court materials, such as motions, orders, and depositions were obtained through PACER. The non-opinions included in the KL3M dataset reflect content from federal courts only.==
### Black's Law Dictionary, 2nd Edition
The second edition of "A Law Dictionary" by Henry Campbell Black, commonly known as "Black's Law Dictionary" is a legal dictionary published in 1910 that contains definitions of the terms and phrases of both ancient and modern American and English jurisprudence.
### U.S. Federal Government Websites
The KL3M dataset includes filtered content from websites of Federal agencies and departments of the United States. ==We filtered content to limit inclusion to only that which is in the public domain.==
### Official Journal of the European Union
The Official Journal of the European Union is the official publication for EU legal acts, other acts and official information from EU institutions, bodies, offices and agencies. 
### US Patent Grant Full Text Data
The United States Patent and Trademark Office (USPTO) publishes the full text, images/drawings, and complex work units (tables, mathematical expressions, chemical structures, and genetic sequence data) of each patent that is granted. The KL3M dataset only contains the full text of granted patents.
## Collection Process
To enable readers to better understand the unique nature of the KL3M dataset, we first outline the steps that we undertook to  determine whether or not a particular data source could be used permissibly. With respect to sources for which the permissibility was positively identified, we describe our technical process for retrieving such data.
### Permissibility of Use
As stated in the introduction, the novel nature of this dataset is the fact that it does not rely on fair use as a basis for establishing permissibility; instead we rely on a multi-part test to determine whether a given data source may be used without restriction. The test is based on a series of conditional assessments: if the data passes a test, we include it in the KL3M dataset; if the data does not pass the test, we move on to the next test. Data that does not pass any of our four tests is not included in the KL3M dataset.
#### Test 1 - Free from Copyright Protection
Our first test is whether the content is free from copyright **at the time of its creation**. Works of the United States government, for example, are not eligible for copyright protection under 17 USC § 105 ("Copyright protection under this title is not available for any work of the United States Government"). Content that meets this test is eligible for inclusion in the KL3M dataset. ==I don't know that it makes sense to list the sources here that meet this test - maybe we have a table later on that shows which test was met?==
#### Test 2 - Public Domain
The second test is whether content has been entered into the public domain or an equivalent, such as a CC0 license where no rights are reserved. Content that has entered the public domain as a result of the lapse of copyright protection falls into this category.
#### Test 3 - Right to Copy, Modify, and Redistribute
If content has not passed the prior two tests, the final test is whether the license grants the right to copy, modify, and redistribute the content without restriction. Licenses that meet this test include CC BY and the United Kingdom's Open Government License (OGL v3.0). If content failed this final test, we did not include it in the KL3M dataset.
#### Excluded Licenses
We excluded the following license types based on the reasoning below: ==(would it be better to have this as a table with categories of reason for exclusion?)==
- CC BY-SA: excluded due to the uncertainty around meeting share-alike obligations with an LLM ==(do we want to talk more about this?)==
- CC BY-NC: excluded due to non-commercial limitation
- CC BY-NC-SA: excluded due to non-commercial limitation and uncertainty around meeting share-alike obligations
- CC BY-ND: excluded due to limitation on derivative work ==(do we want to talk more about this?)==
- CC BY-NC-ND: excluded due to non-commercial and derivative work limitations
### Technical Description (note for readers to check back for update)
How did we actually retrieve the texts?

### Personal Information Considerations
==Personal information (enter stuff about what it is, citations, why it matters).==

The instances of personal information within the KL3M dataset generally arise from their inclusion in documents that are a matter of the public record or that are works of the government. As a result, the personal information that could theoretically be obtained through a review of the KL3M dataset is already publicly available.

CourtListener (and the FreeLaw Project's Board of Directors) has chosen to manage the tension between privacy and the public interest by removing documents from their database only under explicit court order. Their Removal Policy notes:
> We will not remove any public document from our database without a court order. If you want information deleted from our site, your only recourse is to get it deleted from the public record and to obtain a court order demanding that we do the same. If you are able to furnish such a court order, we will generally remove the document from our site. If the court order demands an expungement or redaction, we will generally anonymize or redact cases by replacing names with initials or black boxes, and placing a note at the top of the document explaining the change. We will not make changes to any other documents without a court order that specifically requires that we do so.

### Regulatory Compliance Considerations
==Text about how this will better enable developers who use this dataset to meet regulatory obligations, particularly with respect to transparency== 

## Pre-Processing

## Dataset Characteristics
==Doc length (do we use our tokenizer?), doc type, language distribution; maybe we talk about why we don't have some of the statistics that other datasets/papers do? (e.g., document length - using the US Code as an example)==

### Jurisdictional Coverage
We chose to focus on content related to US, UK, and EU law due to our familiarity with these jurisdictions and the legal intricacies of intellectual property rights. We recognize that this limited coverage does not address much of the world's population and laws, but we hope that the process and tests that we have outlined in this paper will enable others to create similar datasets for additional jurisdictions.


