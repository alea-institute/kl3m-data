<?xml version="1.0" encoding="UTF-8" ?>
<xsl:stylesheet version="1.0" xmlns="http://www.w3.org/1999/xhtml"
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

  <xsl:output method="html" version="4.0" doctype-public="-//W3C//DTD HTML 4.0 Transitional//EN"
  doctype-system="http://www.w3.org/TR/html4/strict.dtd"
  encoding="utf-8" indent="yes"/>

  <xsl:variable name="gpocollection">Federal Register</xsl:variable>
  <xsl:variable name="frnumber" select="FEDREG/NO"/>
  <xsl:variable name="frvolume" select="FEDREG/VOL"/>
  <xsl:variable name="frdate" select="FEDREG/DATE"/>
  <xsl:variable name="frunitname" select="FEDREG/UNITNAME"/>

  <xsl:variable name="UAgpocollection">Unified Agenda</xsl:variable>
  <xsl:variable name="UAfrnumber" select="FEDREG/UNIFIED/NO"/>
  <xsl:variable name="UAfrvolume" select="FEDREG/UNIFIED/VOL"/>
  <xsl:variable name="UAfrdate" select="FEDREG/UNIFIED/DATE"/>
  <xsl:variable name="UAfrunitname" select="FEDREG/UNIFIED/UNITNAME"/>

  <xsl:template match="/">
    <html>
      <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
        <style type="text/css">

		/* document body */
		body { margin-left:50px;margin-right:50px; }

		/* FR Header */
		H1 {font-family:sans-serif;font-weight:bold;font-size:30pt;}
		.FEDREG {font-family:sans-serif;font-size:10pt;}

		/* Unit Headers */
		.FRUNITSTART {margin-top:48pt;margin-bottom:0;display:block;}
		.VOL, .NO {font-weight:bold;}
		.FEDREG-DATE {font-weight:bold;text-align:right;position:absolute;right:50px;}
		.UNITNAME {font-weight:bold;font-size:24pt;text-align:left;margin-bottom:12pt;margin-top:12pt;display:block;}

		/* Page Header */
		.ANCHOR {display:none;}
		/*.PGHEAD {text-align:right;height:30px;display:block;margin-top:28pt;margin-bottom:28pt;margin-left:0pt;margin-right:0pt;text-indent:0cm;font-style:normal;}*/
		.PGHEAD {text-align:right;height:30px;display:block;margin-top:28pt;margin-bottom:28pt;margin-left:0pt;text-indent:0cm;font-style:normal;}
		.PGHDRCOLLECTION {font-weight:bold;}
                /*.PGHDRDLIMIT {text-align:left;font-size:10pt;}*/
		.PGLABEL {font-size:10pt;padding-right:90px;}
		/*.PGLABEL {position:relative;right:50px;font-size:10pt;}*/
		.PRTPAGE {text-align:right;font-weight:bold;position:absolute;right:0px;font-size:11pt;}
		.PRTPAGELN1 {display:block;border-bottom-style:solid;border-width:6px;border-color:black;padding-bottom:3pt;}
		.PRTPAGELN2 {position:absolute;left:50px;right:50px;display:block;border-bottom-style:solid;border-width:1px;border-color:black;margin-bottom:24pt;padding-bottom:3pt;}

		 /* General */
		.E-04 {margin-left:3pt;margin-right:3pt;font-weight:bold;}
		.E-03 {font-style:italic;padding-right:4px;padding-left:4px}
		.E-02 {font-weight:bold;padding-right:4px;padding-left:4px}
		.E-52, .E-54 {font-size:6pt;vertical-align:sub;}
		.APP {margin-top:12pt;margin-bottom:0pt;font-weight:bolder;font-size:12pt;display:block;width:100%;text-align:center;}
		.SU, .E-51, .E-53, .FTREF {font-size:6pt;vertical-align:top;}
		.URL, .E-53, .E-54 {font-style:italic;}

		/* Content, Separate Parts in this Issue, Reader Aids Reference*/
		.AGCY-HD, .PTS-HED, .PTS-HD, .AIDS-HED {margin-top:30pt;margin-bottom:10pt;font-weight:bolder;font-size:12pt;display:block;}
		.CAT-HD {margin-top:4pt;margin-bottom:0pt;font-weight:bolder;display:block;}
		.SEE-HED {font-style:italic;}
		.SEE {margin-top:1pt;margin-bottom:0pt;display:block;}
		.SJ {display:block;}
		.SJDENT {margin-left:10pt;display:block;}
		.SUBSJ {margin-left:20pt;display:block;}
		.SSJDENT {margin-left:35pt;display:block;}
		.PTS, .AIDS {font-family:sans-serif;font-size:10pt;}

		/* ----------- CFR PARTS AFFECTED IN THIS ISSUE ----------- */
		.CPA {margin-top:5pt;margin-bottom:5pt;font-size:10pt;display:block;width:50%;}
		.CPA-HED {margin-top:30pt;margin-bottom:5pt;font-weight:bolder;font-size:12pt;display:block;}
		.CPA-TABLE {width:50%; font-size:12;}
		.CPA-ROW-BOLD {margin-top:3pt;font-weight:bolder;display:block;font-size:12pt}
		.CPA-BOLD {margin-top:2pt;font-weight:bolder;display:block;font-size:10pt}

		/* Rules and Regulations, Proposed Rules */

		/*Modified on Jan 21, 10 nzambrano, first line original*/
		/*.CFR, .SUBJECT, .SUBAGY, .AGENCY, .ACTION, .PART-HED {font-weight:bolder;font-size:12pt;text-align:left;margin-bottom:12pt;margin-top:6pt;display:block;}*/
		.CFR, .SUBJECT, .ACTION {font-weight:bolder;text-align:left;margin-bottom:12pt;margin-top:6pt;display:block;}
		.AGENCY {font-weight:bolder;text-align:left;margin-top:20pt;margin-bottom:10pt;display:block;font-size:12pt;}
		.SUBAGY {font-weight:bolder;text-align:left;margin-bottom:12pt;margin-top:6pt;display:block;font-size:11pt;}

		.NOTE-HED {font-weight:bolder;}
		.AUTHP {font-size:8pt;}
		.FRDOC, .RULE-FRDOC, .FILED {font-size:8pt;display:block;}
		.BILCOD, .RULE-BILCOD {font-size:7pt;font-weight:bolder;display:block;}
		.DEPDOC {font-size:8pt;display:block;font-weight:bolder;}
		.REGTEXT-AUTH {text-indent: 1cm;}
		.STARS {margin-top: 6pt;margin-bottom:6pt;display:block;}
		.GID {font-family:sans-serif;font-size:10pt;margin-top:2pt;margin-bottom:6pt;display:block;}
		.APPENDIX-HD3 , .APPENDIX-FP, .SIG-FP  {font-size:9pt;display:block;}
		.APPENDIX-FP1-2  {font-size:9pt;margin-left:10pt;display:block;}
		.REGTEXT-AMDPAR {font-size:10pt;text-indent: 1cm; margin-top: 8pt; margin-bottom:0;display:block;}
		.P-NMRG {margin-top:0pt;margin-bottom:0;display:block;}
		.SJDENT-SJDOC, .DOCENT-DOC {display:block;float:left;clear:left;}
		.PGS {display:block;}
		.GPH-GID, .MATH-MID {text-align:left;margin-left:2cm;font-size:9pt;font-style:italic;display:block;}

		/*Modified on Jan 21, 10 nzambrano, first line original*/
		/*.SIG-NAME, .APPENDIX-HD1, .SUPLINF-HD1, .EXTRACT-HD1, .PREAMB-HD1, .RESERVED {font-size:9pt;font-weight:bolder;display:block;}*/
		.SIG-NAME, .APPENDIX-HD1, .EXTRACT-HD1, .PREAMB-HD1, .RESERVED {font-size:9pt;font-weight:bolder;display:block;}
		.SIG-TITLE, .APPENDIX-HD2, .SUPLINF-HD2, .PREAMB-HD2 {font-size:9pt;font-style:italic;display:block;}
		.SUPLINF-HD3 {display:block;}
		.SUPLINF-FP {font-size:10pt; margin-left:1cm; margin-top: 4pt; margin-bottom:8pt;display:block;}
		.PREAMB-FP1-2 {margin-left:10pt;}

		/*Modified on Jan 21, 10 nzambrano, first line original*/
		/*.SUPLINF-HD1, .LSTSUB-HED, .APPENDIX-HED, .REGTEXT-HD1 {font-weight:bolder;font-size:11pt;text-align:left;margin-bottom:6pt;margin-top:6pt;display:block;}*/
		.APPENDIX-HED, .REGTEXT-HD1 {font-weight:bolder;font-size:11pt;text-align:left;margin-bottom:6pt;margin-top:6pt;display:block;}
		.SUPLINF-HD1 {font-weight:bolder;text-align:left;margin-bottom:6pt;margin-top:6pt;display:block;}


		/*Modified on Jan 21, 10 nzambrano, first line original*/
		/*.SUPLINF-HED, .RIN, .AGY-HED, .ACT-HED, .SUM-HED, .EFFDATE-HED, .ADD-HED, .FURINF-HED, .AUTH-HED, .E-02, .DATES-HED {font-weight:bolder;font-size:8pt;}*/
		.RIN, .DATES-HED {font-weight:bolder;}
    .DCN, {font-weight:bolder;display:block;}
		.ADD-HED, .SUPLINF-HED, .AGY-HED, .ACT-HED, .SUM-HED, .EFFDATE-HED, .FURINF-HED, .AUTH-HED, .PART-HED, .LSTSUB-HED {font-weight:bolder;text-align:left;margin-bottom:12pt;margin-top:6pt;display:block;}

		.HD1-P {text-indent:1cm;margin-top:4pt;margin-bottom:0pt;display:block;}
		.P {text-indent:1cm;margin-top:4pt;margin-bottom:0pt;display:block;}
		.ADD-EXTRACT {margin-top:0pt;margin-bottom:6pt;display:block;}
		.EXTRACT-FP {margin-top:2pt;margin-bottom:2pt;display:block;}
		.EXTRACT-FP1-2 {margin-top:2pt;margin-left:10pt;margin-bottom:2pt;display:block;}
		.PTITLE-SUBAGY, .PTITLE-CFR {margin-left:30pt;font-weight:bolder;font-size:12pt;text-align:left;margin-bottom:0pt;margin-top:10pt;display:block;}
		.PTITLE-TITLE {margin-left:30pt;font-weight:bolder;font-size:12pt;text-align:left;margin-bottom:0pt;margin-top:0pt;display:block;}
		.FTNT {font-size:8pt;}
		.APPR, .DATED  {text-indent:1cm;margin-top:4pt;margin-bottom:5pt;display:block;}
		.APPRO {margin-top:4pt;margin-bottom:5pt;display:block;}
		.SECTION {margin-top:20pt;}
		.SECTNO {font-weight:bolder;display:block;}
		.SUBCHAP {text-align:center;font-size:15pt;margin-top:20pt;margin-bottom:24pt;display:block;}
		.SUBPART-HED {font-weight:bold;font-size:15pt;margin-top:24pt;margin-bottom:0pt;display:block;}
		.PREAMB-DATE {display:block;font-size:9pt;}

		/* Presidential Documents: Notices, Proclamations, Memos */
		.PTITLE-PARTNO {margin-left:30pt;margin-bottom:14pt;font-size:13pt;font-weight:bolder;display:block;}
		.PTITLE-PRES, .PTITLE-AGENCY {font-family:serif;margin-left:30pt;font-size:24pt;font-weight:bolder;display:block;}
		.PTITLE-PNOTICE, .PTITLE-MEMO, .PTITLE-PROC {margin-left:30pt;font-size:11pt;font-weight:bolder;display:block;}
		.PRESDOCU {margin-top:15pt;display:block;}
		.PRNOTICE-TITLE3, .PROCLA-TITLE3 {font-size:11pt;display:block;font-weight:bolder;margin-bottom:5pt;}
		.PRNOTICE-PRES, .PROCLA-PRES {font-size:13pt;font-weight:bolder;display:block;}
		.PRNOTICE-PNOTICE, .PROCLA-PROC, .PRMEMO-MEMO {margin-left:20%;font-size:11pt;display:block;font-weight:bolder;margin-bottom:5pt;}
		.PRNOTICE-HED, .PROCLA-HED, .PRMEMO-HED {margin-left:20%;font-size:13pt;font-weight:bolder;display:block;}
		.PRNOTICE-FP, .PROCLA-FP, .PRMEMO-FP {margin-left:20%;margin-top:5pt;display:block;}
		.PRNOTICE-PSIG {margin-left:30%;margin-top:10pt;display:block;}
		.PRNOTICE-PLACE, .PRMEMO-PLACE {margin-left:20%;margin-top:10pt;display:block;}
		.PRNOTICE-DATE, .PRMEMO-DATE {margin-left:20%;display:block;font-style:italic;}
		/*.PROCLA-PRES {margin-left:20%;font-size:11pt;font-weight:bolder;display:block;margin-top:18pt;margin-bottom:6pt;}*/
		.PROCLA-GPH, .PRMEMO-GPH, .PRNOTICE-GPH {text-align:right;width:100%;margin-top:18pt;margin-bottom:30pt;}
		.PRMEMO-FRDOC, .PRNOTICE-FRDOC {margin-top:18pt;}

		/* GPO Tables */
		.GPOTABLE {width:100%;margin-left:20pt;margin-top:20pt;margin-bottom:20pt;display:table;border-collapse:collapse;empty-cells:show;}
		.GPOTABLE-TTITLE {padding:5px;text-align:center;width:100%;display:block;font-weight:bold;}
		/*.CHED {font-size:8pt;padding:5px;font-weight:bold;border-left-style:solid;bold;border-right-style:solid;bold;border-top-style:solid;border-bottom-style:solid;border-width:1px; border-color:black;}*/
		.CHED-LI {display:block;}
		.CHED {font-size:8pt;padding:5px;font-weight:bold;border-top-style:solid;border-bottom-style:solid;border-width:1px;border-color:black;}
		.GPOHEADERS {font-weight:bold;font-size:9pt;text-align:center;border-left-style:solid;border-right-style:solid;border-width:1px;border-bottom-style:solid;border-top-style:solid;border-width:1px;border-color:black;}
		.GPOH2HEADERS {font-weight:bold;font-size:9pt;text-align:center;border-left-style:solid;border-right-style:solid;border-width:1px;border-bottom-style:solid;border-top-style:solid;border-width:1px;border-color:black;}
		.GPOHEADERS:first-child {border-left:none;}
		.GPOHEADERS:last-child, .GPOH2HEADERS:last-child {border-right:none;}
		.ENT {font-size:8pt;padding:5px;border-left-style:solid;border-right-style:solid;border-top-style:none;border-bottom-style:none;border-width:1px; border-color:black;}
		.ENT:first-child {border-left:none;}
		.ENT:last-child {border-right:none;}
		.ROW:last-child > .ENT {border-bottom-style:solid; }
		#ENT {font-size:8pt;padding:5px;border-left-style:dotted;border-right-style:dotted;border-top-style:dotted;border-bottom-style:dotted;border-width:1px; border-color:black;}
		.MyENT {visibility:hidden; font-size:8pt;padding:5px;border-left-style:solid;border-right-style:solid;border-top-style:none;border-bottom-style:none;border-width:1px; border-color:black;}
                .BOXHD {width:100%;}
                .ROW {width:100%;}
                #ROW {width:100%;}
                .UA-ROW-ENT,.ROW-ENT {display:none}
		.TNOTE {font-size:8pt;padding-left:15px;}
		.TDPRTPAGE {width:100%;}
		.TRPRTPAGE {width:100%;display:block;}

		tr:not(.ROW) > .TNOTE {border-top-style:solid;border-width:1px;padding-top:1em;}
		tr:not(.ROW) + tr:not(.ROW) > .TNOTE {border-top-style:none;padding-top:3pt;}

		.ROW.ROW-RUL-NSBAR > .ENT, .ROW.ROW-RUL-SBAR > .ENT {border-bottom-style:solid;}
		.ROW.ROW-RUL-NSBAR > .ENT:first-child {border-bottom-style:none;}

		/* Set without test-against PDF sample.*/
		.ADMIN-HD  {font-weight:bolder;font-size:12pt;display:block;}
		.ADMIN-HED, .ED-HED, .ANNEX-HED, .BRIEFBOX-HED, .CROSSREF-HED, .EBB-HED, .EDNOTE-HED, .EFFDNOT-HED, .PAGDATE-HED, .PREAMHD-HED {font-weight:bolder;font-size:8pt;}
		.ADMIN-HD1, .CONTENTS-HD1, .EFFDNOT-HD1, .PART-HD1 {font-size:9pt;font-weight:bolder;display:block;}
		.AGENCIES  {font-weight:bolder;font-size:12pt;text-align:left;margin-bottom:12pt;margin-top:6pt;display:block;}
		.CITA {text-indent: 1cm; margin-top: 4pt; margin-bottom:0;display:block;}
		.CONTENTS-HD2, .CONTENTS-HD3, .EFFDNOT-HD2, .EFFDNOT-HD3, .PART-HD2, .PART-HD3, .PREAMB-HD3 {font-size:9pt;font-weight:bolder;display:block;}
		.EXHIBIT-NAME, .EXTRACT-NAME {font-size:9pt;font-weight:bolder;display:block;}

			/*
			   Other Tag Notes
						AC: Ignored, examples on PDF have no distinction with other text.
						 E: Attribute needs to be decoded, not handling all potential variations.
						FR: Ignored, fraction function, no formatting on PDF.
				 FTREF:	SU tag is making the formatting, redundant, ignoring.
				    FP: Did not find mechanism to indent all lines except first. By default formatted as non-indented paragraph.
				  SECT, DOC: default formatting.
          WIDE, WSECT: force two-column, not relevant on XHTML.
          PLACE, PNOTICE, PRES, PSIG, TITLE, TITLE3: Need to check if additional sub-element variations on containers.

            ED, CONTENTS, BRIEFBOX, CNTNTS, CORRECT, CUMLIST, CXPAGE, REGTEXT, DOCENT, EFFDNOT, EXAMPLE, FURINF, GPH,
            NEWPART, NOTE, NOTES, NOTICE, NOTICES, OLNOTES, PAGDATE, PART, PREAMB, PRESDOC, PRESDOCS, PRESDOCU, PRNOTICE,
            PRORULE, PRORULES, PTITLE, RULE, RULES, SCOL2, SUBPART, SUM, SUPLINF,  :
              Container, Assuming Sub-Element Format

           ACCESS, ADOPT, H, ADMIN, FRPAGE, BQUOTE, ANOTICE, ATNAME, ATTEST, READAID, SECTNAME, CFRPART, CFRPARTS, PRE,
           BTITLE, CFRS, CFRSET, CHAPS, CHECKLST, CITY, COMRULE, DOCKETHD, EFFDATES, ELECBB, EX, EXEC, FL-2, FRDATE, GPO,
           SIGDAT, HJRNO, HNO, HRNO, INDXAIDS, INFO, INFOASST, LASTLIST, LAWSLIST, LDRFIG, LDRWK, LISTING, LOPL, LSER,
           TCAP, BCAP, MEMS, MICROED, MISCPUBS, MOREPGS, NEWBOOKT, NEXTBOX, ORDER, ORDERNO, PARAUTH, PARTS, PARTSAFF,
           PENS, PHONENO, PRESDET, PRICE, PROCNO, PROCS, PRORDER, PUBLAND, PUBLANDO, PUBLAWS, REMINDER, RESERVA, REVDATE,
           REVTXT, RULEHED, SET, SFP-2, SITE, SJRNO, SN, SNO, SOURCE, SRNO, SUBDAT, SUBDOC, SUBSCRIP, SUBTITLE, SYMBOL,
           TITLENO, TITLEPAG, TOEDATP, WHEN, WHERE, WORKSHOP:
              No Format Examples, Assuming Sub-Element Format or Default Formatting
			*/

		/* UNIFIED */

		.UA-ABSTR{font-weight:bolder;display:block;float:left;clear:left;margin-right:5px;}
		.UA-ACRONYM{display:none;}
		.UA-ACT{display:block;margin-top:12px;margin-bottom:12px;width:100%;}
		.UA-ACT-HEAD{display:inline;}
		.UA-ACT-HED{font-weight:bolder;display:block;float:left;clear:left;margin-right:5px;}
		.UA-ADD-EXTRACT{margin-top:0pt;margin-bottom:6pt;display:block;}

		.UA-ADD-HED{font-weight:bolder;display:block;float:left;clear:left;margin-right:5px;}

		.UA-ADDINFO{font-weight:bolder;display:block;float:left;clear:left;margin-right:5px;}
		.UA-ADMIN-HD{font-weight:bolder;font-size:12pt;display:block;}
		.UA-ADMIN-HD1{font-size:9pt;font-weight:bolder;display:block;}
		.UA-ADMIN-HED{font-weight:bolder;font-size:8pt;}
		.UA-AGCY{font-weight:bolder;font-size:14pt;}
		.UA-REGPLAN-AGCY {margin-bottom:20pt;display:block;}
		.UA-REGPLAN-TITLE {font-weight:bolder;}
		.UA-REGPLAN-SEQNO {font-weight:bolder;}
		.UA-SONEED, .UA-LEGBASIS, .UA-ALTS, .UA-COSTBEN, .UA-RISKS {font-weight:bolder;}
		.UA-AGCY-HD{margin-top:12pt;margin-bottom:0pt;font-weight:bolder;font-size:12pt;display:block;}
		.UA-AGENCIES{font-weight:bolder;font-size:12pt;text-align:left;margin-bottom:12pt;margin-top:6pt;display:block;}
		.UA-AGENCY{font-weight:bolder;font-size:12pt;text-align:left;margin-bottom:12pt;margin-top:6pt;display:block;}
		.UA-AGY{display:block;margin-top:12px;margin-bottom:12px;width:100%;}
		.UA-AGY-HED{font-weight:bolder;display:block;float:left;clear:left;margin-right:5px;}
		.UA-AIDS{font-family:sans-serif;font-size:10pt;}
		.UA-AIDS-HED{margin-top:12pt;margin-bottom:0pt;font-weight:bolder;font-size:12pt;display:block;}
		.UA-ALPHHD{font-weight:bolder;font-size:12pt;text-align:center;width:80%;display:block;margin-top:2cm;}
		.UA-ANNEX-HED{font-weight:bolder;font-size:8pt;}
		.UA-APP{margin-top:12pt;margin-bottom:0pt;font-weight:bolder;font-size:12pt;display:block;width:100%;text-align:center;}
		.UA-APPENDIX-FP{font-size:9pt;display:block;}
		.UA-APPENDIX-HD1{font-size:9pt;font-weight:bolder;display:block;}
		.UA-APPENDIX-HD2{font-size:9pt;font-style:italic;display:block;}
		.UA-APPENDIX-HD3{font-size:9pt;display:block;}
		.UA-APPENDIX-HED{font-weight:bolder;font-size:11pt;text-align:left;margin-bottom:6pt;margin-top:6pt;display:block;}
		.UA-APPR{text-indent:1cm;margin-top:4pt;margin-bottom:5pt;display:block;}
		.UA-APPRO{margin-top:4pt;margin-bottom:5pt;display:block;}
		/*.UA-AUTH-HED{font-weight:bolder;}*/
		.UA-AUTH-HED {font-weight:bolder;display:block;float:left;clear:left;margin-right:5px;}
		.UA-AUTHP{font-size:8pt;}
		.UA-BILCOD{font-size:7pt;font-weight:bolder;display:block;}
		.UA-BRANCH-AGCY{font-weight:bolder;float:left;width:80%;}
		.UA-BRANCH-CFR{font-weight:bolder;display:block;float:left;clear:left;margin-right:5px;}
		.UA-BRANCH-REGFLEX{font-weight:bolder;display:block;float:left;clear:left;margin-right:5px;}
		.UA-BRANCH-REGULAT{font-weight:bolder;display:block;float:left;clear:left;margin-right:5px;}
		.UA-BRANCH-SECTORS{font-weight:bolder;display:block;float:left;clear:left;margin-right:5px;}
		.UA-BRANCH-SEQNO{font-weight:bolder;display:block;float:left;clear:left;font-size:12pt;margin-right:5px;}
		.UA-BRANCH-SMALLENT{font-weight:bolder;display:block;float:left;clear:left;margin-right:5px;}
		.UA-BRANCH-SUBAGY{margin-bottom:10pt;margin-top:2pt;display:block;text-align:left;}
		.UA-BRANCH-TITLE{font-weight:bolder;display:block;width:90%;font-size:12pt;}
		.UA-BRIEFBOX-HED{font-weight:bolder;font-size:8pt;}
		.UA-CA{font-weight:bolder;font-size:14pt;text-align:right;width:100%;}
		.UA-CAT-HD{margin-top:4pt;margin-bottom:0pt;font-weight:bolder;font-size:8pt;display:block;}
		.UA-ABSTR-PADDING {margin-bottom:15px;}
		.UA-COMPLETD{display:block;margin-bottom:10px;margin-top:10px;padding:0px;table-layout:fixed;}
		.UA-COMPLETD CAPTION{font-weight:bolder;display:block;text-align:left;}
		.UA-COMPLETD TH{font-weight:bolder;font-size:10pt;text-align:left;border-top-width:thin;border-bottom-width:thin;border-top-style:solid;border-bottom-style:solid;border-top-border-bottom-padding:2px 10px 2px 0px;}
		.UA-COMPLETD TD{text-align:left;padding-right:10px;font-size:10pt;}
		.UA-COMPLETD-CITA{left:600px;position:absolute;}
		.UA-COMPLETD-DATE{left:450px;font-weight:normal;position:absolute;}
		.UA-COMPLETD-HED{font-weight:bolder;font-size:12pt;}
		.UA-CONTACT{font-weight:bolder;display:block;float:left;clear:left;margin-right:5px;}
		.UA-CONTENTS-HD1{font-size:9pt;font-weight:bolder;display:block;}
		.UA-CONTENTS-HD2{font-size:9pt;font-weight:bolder;display:block;}
		.UA-CONTENTS-HD3{font-size:9pt;font-weight:bolder;display:block;}
		.UA-CROSSREF-HED{font-weight:bolder;font-size:8pt;}
		.UA-DATED-PADDING{margin-bottom:0px;}
		.UA-DATES-HED{font-weight:bolder;}
		.UA-DEPDOC{font-size:8pt;display:block;font-weight:bolder;}
		.UA-E-02{font-weight:bolder;padding-right:4px;padding-left:4px;}
		.UA-E-03 {font-style:italic;padding-right:4px;padding-left:4px}
		.UA-E-04{margin-left:3pt;margin-right:3pt;font-weight:bolder;}
		.UA-E-51{font-size:6pt;vertical-align:top;}
		.UA-E-52{font-size:6pt;vertical-align:sub;}
		.UA-EBB-HED{font-weight:bolder;font-size:8pt;}
		.UA-ED-HED{font-weight:bolder;font-size:8pt;}
		.UA-EDNOTE-HED{font-weight:bolder;font-size:8pt;}
		/*.UA-EFFDATE-HED{font-weight:bolder;}*/
		.UA-EFFDATE-HED {font-weight:bolder;display:block;float:left;clear:left;margin-right:5px;}
		.UA-EFFDNOT-HD1{font-size:9pt;font-weight:bolder;display:block;}
		.UA-EFFDNOT-HD2{font-size:9pt;font-weight:bolder;display:block;}
		.UA-EFFDNOT-HD3{font-size:9pt;font-weight:bolder;display:block;}
		.UA-EFFDNOT-HED{font-weight:bolder;font-size:8pt;}
		.UA-EXHIBIT-NAME{font-size:9pt;font-weight:bolder;display:block;}
		.UA-EXTRACT-FP{margin-top:2pt;margin-bottom:2pt;display:block;}
		.UA-EXTRACT-HD1{font-size:9pt;font-weight:bolder;display:block;}
		.UA-EXTRACT-NAME{font-size:9pt;font-weight:bolder;display:block;}
		.UA-FEDLISM{font-weight:bolder;display:block;float:left;clear:left;}
		.UA-FEDREG{font-family:sans-serif;font-size:10pt;}
		.UA-FEDREG-DATE{font-weight:bolder;text-align:right;position:absolute;right:50px;}
		.UA-FILED{font-size:8pt;display:block;}
		.UA-FINRS{font-weight:bolder;font-size:14pt;text-align:right;width:100%;margin-bottom:10pt;margin-top:10pt;}
		.UA-FP{display:block;margin-right:5px;margin-bottom:10px;margin-top:10px;}
		.UA-FRDOC{font-size:8pt;display:block;}
		.UA-FRUNITSTART{margin-top:48pt;margin-bottom:0;display:block;}
		.UA-FTNT{font-size:8pt;}
		.UA-FTREF{font-size:6pt;vertical-align:top;}
		.UA-FURINF{display:block;margin-top:12px;margin-bottom:12px;width:100%;}
		.UA-FURINF-HED{font-weight:bolder;display:block;float:left;clear:left;margin-right:5px;}
		.UA-GID{font-family:sans-serif;font-size:10pt;margin-top:2pt;margin-bottom:6pt;display:block;}
		.UA-GOVTLEVS{font-weight:bolder;display:block;float:left;clear:left;margin-right:5px;}
		.UA-GPOTABLE-TTITLE{display:block;font-size:14pt;text-align:left;margin-top:20px;margin-bottom:20px;width:100%;}
		.UA-HD1-P{text-indent:1cm;margin-top:4pt;margin-bottom:0pt;display:block;}
		.UA-HD1-PADDING{margin-bottom:10px;}
		.UA-HED-PADDING{margin-bottom:10px;}
		.UA-LEGAUTH{font-weight:bolder;display:block;float:left;clear:left;margin-right:5px;}
		.UA-LEGDEAD{font-weight:bolder;display:block;float:left;clear:left;margin-right:5px;}
		.UA-LSTSUB-HED{font-weight:bolder;text-align:left;margin-bottom:6pt;margin-top:6pt;display:block;}
		.UA-BRANCH-LTA{font-weight:bolder;font-size:14pt;text-align:right;width:100%;}
		.UA-NO{font-weight:bolder;}
		.UA-NOTE-HED{font-weight:bolder;}
		.UA-P{display:block;width:90%;margin-bottom:3px;}
		.UA-PADDING{display:block;width:80%;margin-left:15%;}
		.UA-PAGDATE-HED{font-weight:bolder;font-size:8pt;}
		.UA-PART-HD1{font-size:9pt;font-weight:bolder;display:block;}
		.UA-PART-HD2{font-size:9pt;font-weight:bolder;display:block;}
		.UA-PART-HD3{font-size:9pt;font-weight:bolder;display:block;}
		.UA-PART-HED{text-align:left;margin-bottom:12pt;margin-top:6pt;display:block;}
		.UA-PGHDRCOLLECTION{font-weight:bolder;}
		.UA-PGHEAD{display:block;clear:left;float:left;width:100%;margin-top:24pt;margin-bottom:0pt;margin-left:0pt;margin-right:0pt;text-indent:0cm;font-style:normal;}
		.UA-PGLABEL{text-align:left;font-size:10pt;}
		.UA-P-NMRG{margin-top:0pt;margin-bottom:0;display:block;}
		.UA-PREAMB-DATE{display:block;font-size:9pt;}
		.UA-PREAMB-HD1{font-size:9pt;font-weight:bolder;display:block;}
		.UA-PREAMB-HD2{font-size:9pt;font-style:italic;display:block;}
		.UA-PREAMB-HD3{font-size:9pt;font-weight:bolder;display:block;}
		.UA-PREAMHD-HED{font-weight:bolder;font-size:8pt;}
		.UA-PRELIM-AGCY{font-weight:bolder;font-size:12pt;text-align:left;margin-bottom:12pt;margin-top:6pt;display:block;}
		.UA-PRELIM-CFR{font-weight:bolder;text-align:left;margin-top:10pt;display:block;margin-bottom:10px;}
		.UA-PRELIM-SUBAGY{font-weight:bolder;text-align:left;margin-top:10pt;display:block;margin-bottom:10px;}
		.UA-PRELIM-SUBJECT{font-weight:bolder;text-align:left;margin-top:10pt;display:block;margin-bottom:10px;}
		.UA-PRERS{font-weight:bolder;font-size:14pt;text-align:right;width:100%;}
		.UA-PRESDOCU{margin-top:15pt;display:block;}
		.UA-PRIOR{font-weight:bolder;display:block;float:left;clear:left;margin-right:5px;margin-top:10pt;}
		.UA-REGPLAN-FP {margin-top:10px;}
		.UA-PRMEMO-DATE{margin-left:20%;display:block;font-style:italic;}
		.UA-PRMEMO-FP{margin-left:20%;margin-top:5pt;display:block;}
		.UA-PRMEMO-FRDOC{margin-top:18pt;}
		.UA-PRMEMO-GPH{text-align:right;width:100%;margin-top:18pt;margin-bottom:30pt;}
		.UA-PRMEMO-HED{margin-left:20%;font-size:13pt;font-weight:bolder;display:block;}
		.UA-PRMEMO-MEMO{margin-left:20%;font-size:11pt;display:block;font-weight:bolder;margin-bottom:5pt;}
		.UA-PRMEMO-PLACE{margin-left:20%;margin-top:10pt;display:block;}
		.UA-PRNOTICE-DATE{margin-left:20%;display:block;font-style:italic;}
		.UA-PRNOTICE-FP{margin-left:20%;margin-top:5pt;display:block;}
		.UA-PRNOTICE-FRDOC{margin-top:18pt;}
		.UA-PRNOTICE-GPH{text-align:right;width:100%;margin-top:18pt;margin-bottom:30pt;}
		.UA-PRNOTICE-HED{margin-left:20%;font-size:13pt;font-weight:bolder;display:block;}
		.UA-PRNOTICE-PLACE{margin-left:20%;margin-top:10pt;display:block;}
		.UA-PRNOTICE-PNOTICE{margin-left:20%;font-size:11pt;display:block;font-weight:bolder;margin-bottom:5pt;}
		.UA-PRNOTICE-PRES{font-size:13pt;font-weight:bolder;display:block;}
		.UA-PRNOTICE-PSIG{margin-left:30%;margin-top:10pt;display:block;}
		.UA-PRNOTICE-TITLE3{font-size:11pt;display:block;font-weight:bolder;margin-bottom:5pt;}
		.UA-PROCLA-FP{margin-left:20%;margin-top:5pt;display:block;}
		.UA-PROCLA-GPH{text-align:right;width:100%;margin-top:18pt;margin-bottom:30pt;}
		.UA-PROCLA-HED{margin-left:20%;font-size:13pt;font-weight:bolder;display:block;}
		.UA-PROCLA-PRES{font-size:13pt;font-weight:bolder;display:block;}
		.UA-PROCLA-PROC{margin-left:20%;font-size:11pt;display:block;font-weight:bolder;margin-bottom:5pt;}
		.UA-PROCLA-TITLE3{font-size:11pt;display:block;font-weight:bolder;margin-bottom:5pt;}
		.UA-PRORS{font-weight:bolder;font-size:14pt;text-align:right;width:100%;margin-top:10pt;}
		.UA-PRTPAGE{text-align:right;font-weight:bolder;position:absolute;right:50px;font-size:11pt;}
		.UA-PRTPAGELN1{width:100%;border-bottom-style:solid;border-width:6px;border-color:black;padding-bottom:3pt;}
		.UA-PRTPAGELN2{width:100%;border-bottom-style:solid;border-width:1px;border-color:black;margin-bottom:24pt;padding-bottom:3pt;}
		.UA-PTITLE-AGCY{font-weight:bolder;display:block;margin-top:12px;margin-bottom:12px;margin-left:1cm;font-size:16pt;text-decoration:underline;}
		.UA-PTITLE-AGENCY{font-family:serif;margin-left:30pt;font-size:24pt;font-weight:bolder;display:block;}
		.UA-PTITLE-CFR{margin-left:30pt;font-weight:bolder;font-size:12pt;text-align:left;margin-bottom:0pt;margin-top:10pt;display:block;}
		.UA-PTITLE-MEMO{margin-left:30pt;font-size:11pt;font-weight:bolder;display:block;}
		.UA-PTITLE-PARTNO{margin-left:30pt;margin-bottom:14pt;font-size:13pt;font-weight:bolder;display:block;}
		.UA-PTITLE-PNOTICE{margin-left:30pt;font-size:11pt;font-weight:bolder;display:block;}
		.UA-PTITLE-PRES{font-family:serif;margin-left:30pt;font-size:24pt;font-weight:bolder;display:block;}
		.UA-PTITLE-PROC{margin-left:30pt;font-size:11pt;font-weight:bolder;display:block;}
		.UA-PTITLE-SUBAGY{margin-left:30pt;font-weight:bolder;font-size:12pt;text-align:left;margin-bottom:0pt;margin-top:10pt;display:block;}
		.UA-PTITLE-TITLE{margin-left:30pt;font-weight:bolder;font-size:12pt;text-align:left;margin-bottom:0pt;margin-top:0pt;display:block;}
		.UA-PTS{font-family:sans-serif;font-size:10pt;}
		.UA-PTS-HD{margin-top:12pt;margin-bottom:0pt;font-weight:bolder;font-size:12pt;display:block;}
		.UA-PTS-HED{margin-top:12pt;margin-bottom:0pt;font-weight:bolder;font-size:12pt;display:block;}
		.UA-REGTEXT-AMDPAR{font-size:10pt;text-indent:1cm;margin-top:8pt;margin-bottom:0;display:block;}
		.UA-REGTEXT-AUTH{text-indent:1cm;}
		.UA-REGTEXT-HD1{font-weight:bolder;font-size:11pt;text-align:left;margin-bottom:6pt;margin-top:6pt;display:block;}
		.UA-RESERVED{font-size:9pt;font-weight:bolder;display:block;}
		.UA-RIGHTPADDING{margin-right:15%;}
		.UA-RIN{font-weight:bolder;display:block;float:left;clear:left;margin-right:5px;}
		.UA-RIN-PADDING{display:block;width:90%;margin-bottom:3px;}
		.UA-RULE-BILCOD{font-size:7pt;font-weight:bolder;display:block;}
		.UA-RULE-FRDOC{font-size:8pt;display:block;}
		.UA-SECTION{margin-top:20pt;}
		.UA-SECTNO{font-weight:bolder;display:block;}
		.UA-SEE{margin-top:1pt;margin-bottom:0pt;display:block;}
		.UA-SEEALSO:before{content:"See also ";font-style:italic;}
		.UA-SEEALSO{display:block;clear:both;margin-left:0.5cm;}
		.UA-SEE-HED{font-style:italic;}
		.UA-SIG-DATED{font-weight:bolder;display:block;float:left;clear:left;margin-right:5px;}
		.UA-SIG-FP{font-size:9pt;display:block;}
		.UA-SIG-NAME{font-weight:bolder;display:block;}
		.UA-SIG-TITLE{font-style:italic;display:block;}
		.UA-SJ{display:block;}
		.UA-SJDENT{margin-left:10pt;display:block;}
		.UA-SSJDENT{margin-left:35pt;display:block;}
		.UA-STARS{margin-top:6pt;margin-bottom:6pt;display:block;}
		.UA-SU{font-size:6pt;vertical-align:top;}
		.UA-SUBAGCY-CFR{font-weight:bolder;display:block;float:left;clear:left;}
		.UA-SUBAGCY-REGFLEX{font-weight:bolder;display:block;float:left;clear:left;margin-right:5px;}
		.UA-SUBAGCY-SEQNO{font-weight:bolder;display:block;float:left;clear:left;font-size:12pt;margin-right:5px;}
		.UA-SUBAGCY-SMALLENT{font-weight:bolder;display:block;float:left;clear:left;margin-right:5px;}
		.UA-SUBAGCY-SUBAGY{font-weight:bolder;font-size:12pt;margin-bottom:10pt;margin-top:2pt;display:block;text-align:left;}
		.UA-SUBAGCY-TITLE{font-weight:bolder;display:block;width:90%;font-size:12pt;}
		.UA-SUBCHAP{text-align:center;font-size:15pt;margin-top:20pt;margin-bottom:24pt;display:block;}
		.UA-SUBJ1L{display:block;clear:both;float:left;margin-left:0.5cm;}
		.UA-SUBJ2L{display:block;clear:both;float:left;margin-left:1.0cm;}
		.UA-SUBJECT1{display:block;clear:both;float:left;margin-left:0.5cm;}
		.UA-SUBJIND-HED{display:block;margin-top:5px;font-size:14pt;font-weight:bolder;}
		.UA-SUBJIND-SEQNO{display:block;clear:right;float:right;}
		.UA-SUBJIND-SUBJECT{display:block;clear:both;}
		.UA-SUBPART-HED{font-weight:bolder;font-size:15pt;margin-top:24pt;margin-bottom:0pt;display:block;}
		.UA-SUBSJ{margin-left:20pt;display:block;}
		.UA-SUM{display:block;margin-top:12px;margin-bottom:12px;width:100%;}
		.UA-SUM-FP-1{display:block;}
		.UA-SUM-HED{font-weight:bolder;display:block;float:left;clear:left;margin-right:5px;}
		.UA-SUPLINF{display:block;margin-top:12px;margin-bottom:12px;width:100%;}
		.UA-SUPLINF-FP{font-size:10pt;margin-left:1cm;margin-top:4pt;margin-bottom:8pt;display:block;}
		.UA-SUPLINF-HD1{font-weight:bolder;font-size:10pt;text-align:left;margin-bottom:6pt;margin-top:6pt;display:block;width:100%;}
		.UA-SUPLINF-HD2{font-size:9pt;font-style:italic;display:block;}

		.UA-SUPLINF-HED{font-weight:bolder;display:block;float:left;clear:left;margin-right:5px;}

		.UA-TDPRTPAGE{width:100%;}
		.UA-TIMETBL{display:block;margin-bottom:10px;margin-top:20px;padding:0px;table-layout:auto;}
		.UA-TIMETBL CAPTION{font-weight:bolder;display:block;text-align:left;}
		.UA-TIMETBL TH{font-weight:bolder;font-size:10pt;text-align:left;border-top-width:thin;border-bottom-width:thin;border-top-style:solid;border-bottom-style:solid;border-top-border-bottom-padding:2px 10px 2px 0px;}
		.UA-TIMETBL TD{text-align:left;padding-right:10px;font-size:10pt;}
		.UA-COL-ACTION{width:50%;}
		.UA-COL-DATE{width:25%;}
		.UA-COL-CITA{width:25%;}
		.COMPLETD-HED, .TIMETBL-HED{font-weight:bolder;font-size:12pt;margin-top:12pt;}
		.UA-TIMETBL-CITA{left:600px;position:absolute;}
		.UA-TIMETBL-DATE{left:450px;font-weight:normal;position:absolute;}
		.UA-TIMETBL-HED{font-weight:bolder;font-size:12pt;}
		.UA-TRPRTPAGE{width:100%;}
		.UA-UNIFIED-DATE{font-weight:bolder;text-align:right;position:absolute;right:50px;}
		.UA-UNITNAME{font-weight:bolder;font-size:14pt;text-align:left;margin-bottom:12pt;margin-top:12pt;display:block;}
		.UA-URL{font-style:italic;}
		.UA-VOL{font-weight:bolder;}
		.UNIFIED{font-family:sans-serif;font-size:10pt;}

        </style>
      </head>
      <body>
        <xsl:apply-templates/>
      </body>
    </html>
  </xsl:template>

  <!-- Tags being Ignored -->
  <xsl:template match="INCLUDES | EDITOR | EAR | FRDOCBP | HRULE | FTREF | NOLPAGES | OLPAGES | NOPRINTSUBJECT | NOPRINTEONOTES">
  </xsl:template>

  <xsl:template match="FEDREG">
    <span>
      <xsl:attribute name="class">
        <xsl:value-of select="name()"/>
      </xsl:attribute>
      <xsl:apply-templates/>
    </span>
  </xsl:template>

  <xsl:template match="VOL">
    <p class="FRUNITSTART"/>
    <span class="VOL">Vol. <xsl:value-of select="."/>, </span>
  </xsl:template>

  <xsl:template match="NO">
    <span class="NO">No. <xsl:value-of select="."/></span>
  </xsl:template>

  <xsl:template match="UNITNAME">
    <hr/>
    <xsl:call-template name="apply-span"/>
    <hr/>
  </xsl:template>

  <xsl:template match="UNIFIED/AGENDA/BRANCH/SEQNO | UNIFIED/AGENDA/BRANCH/SUBAGCY/SEQNO | UNIFIED/REGPLAN/SEQNO">
    <xsl:variable name="seqnum" select="."/>
      <a>
        <xsl:attribute name="name">
          <xsl:text>seqnum</xsl:text><xsl:value-of select='translate($seqnum, ".", "")'/>
        </xsl:attribute>
      </a>
    <xsl:call-template name="apply-span"/>
  </xsl:template>

 <!--
  <xsl:template match="UNIFIED/PREAMBLE/SUPLINF/HD">
     <xsl:choose>
        <xsl:when test="./@SOURCE=HD1">
           <xsl:variable name="seqnum" select="."/>
          <a>
            <xsl:attribute name="name">
              <xsl:text>seqnum</xsl:text><xsl:value-of select='translate($seqnum, ".", "")'/>
            </xsl:attribute>
          </a>
        </xsl:when>
	<xsl:otherwise>
	  <xsl:call-template name="apply-span"/>
	</xsl:otherwise>
    </xsl:choose>
  </xsl:template>
 -->

  <xsl:template match="UNIFIED/AGENDA/BRANCH/TITLE | UNIFIED/AGENDA/BRANCH/SUBAGCY/TITLE">
      <xsl:call-template name="apply-span"/>
      <br/>
  </xsl:template>

  <xsl:template match="RULE | PRORULE | NOTICE ">
    <xsl:variable name="nodename" select="name()"/>
    <xsl:choose>
      <xsl:when test="preceding-sibling::*">
        <hr/>
        <xsl:call-template name="apply-span"/>
      </xsl:when>
      <xsl:otherwise>
        <xsl:call-template name="apply-span"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template match="PTS | AIDS">
    <hr/>
    <xsl:call-template name="apply-span"/>
  </xsl:template>

  <xsl:template match="SIG/FP | SIG/NAME | SIG/TITLE">
    <xsl:call-template name="apply-span"/>
    <p class="P-NMRG" />
  </xsl:template>

  <xsl:template match="PRTPAGE">
     <xsl:choose>
       <xsl:when test="ancestor::FEDREG">
        <xsl:choose>
          <xsl:when test="parent::ROW and not(preceding-sibling::ENT)">
            <xsl:variable name="columns" select="(ancestor::GPOTABLE)[last()]/@COLS"/>
            <tr class="TRPRTPAGE">
              <td class="TDPRTPAGE">
                <xsl:attribute name="colspan">
                  <xsl:value-of select="$columns"/>
                </xsl:attribute>
                <xsl:call-template name="apply-pgheader"/>
              </td>
            </tr>
          </xsl:when>
          <xsl:otherwise>
            <xsl:call-template name="apply-pgheader"/>
          </xsl:otherwise>
        </xsl:choose>
      </xsl:when>
    </xsl:choose>
  </xsl:template>

  <xsl:template match="UNIFIED/AGENDA/BRANCH/AGCY">
      <br/><hr/><xsl:call-template name="apply-span"/>
  </xsl:template>

  <xsl:template match="UNIFIED/AGENDA/BRANCH/SUBAGY">
      <xsl:call-template name="apply-span"/><hr/><br/>
  </xsl:template>

  <xsl:template name="apply-pgheader">
     <xsl:choose>
       <xsl:when test="ancestor::FEDREG">
        <xsl:choose>
          <xsl:when test="./@P">
            <xsl:variable name="pagenum" select="./@P"/>
            <!--span class="ANCHOR"-->
             <a>
	        <xsl:attribute name="name">
	           <xsl:text>seqnum</xsl:text><xsl:value-of select="$pagenum"/>
	        </xsl:attribute>
             </a>
            <!--/span-->
            <span class="PGHEAD">
              <span class="PRTPAGELN2">
                <span class="PRTPAGELN1">
                <span class="PGLABEL">
                  <span class="PGHDRCOLLECTION">
                    <xsl:choose>
                      <xsl:when test="ancestor::UNIFIED"><xsl:value-of select="$UAgpocollection"/></xsl:when>
                      <xsl:otherwise><xsl:value-of select="$gpocollection"/></xsl:otherwise>
                    </xsl:choose>
                  </span>
                  <span class="PGHDRDLIMIT"><xsl:text> / </xsl:text></span>
                  <span class="PGHDRREFERENCE">
                    <xsl:choose>
                      <xsl:when test="ancestor::UNIFIED">
                        Vol. <xsl:value-of select="$UAfrvolume"/>,
                        <xsl:text> </xsl:text>
                        No. <xsl:value-of select="$UAfrnumber"/>
                        <xsl:text> </xsl:text>
                      </xsl:when>
                      <xsl:otherwise>
                        Vol. <xsl:value-of select="$frvolume"/>,
                        <xsl:text> </xsl:text>
                        No. <xsl:value-of select="$frnumber"/>
                        <xsl:text> </xsl:text>
                      </xsl:otherwise>
                    </xsl:choose>
                  </span>
                  <span class="PGHDRDLIMIT"><xsl:text> / </xsl:text></span>
                  <span class="PGHDRDATE">
                    <xsl:choose>
                      <xsl:when test="ancestor::UNIFIED">
                        <xsl:value-of select="$UAfrdate"/>
                      </xsl:when>
                      <xsl:otherwise>
                        <xsl:value-of select="$frdate"/>
                      </xsl:otherwise>
                    </xsl:choose>
                  </span>
                  <!--
                    <span class="PGHDRDLIMIT"><xsl:text> / </xsl:text></span>
                    <span class="PGHDRUNITNAME">
                    <xsl:value-of select="$frunitname"/>
                    </span>
                  -->
                </span>
                <span>
                  <xsl:attribute name="class">
                    <xsl:value-of select="name()"/>
                  </xsl:attribute>
                  <xsl:value-of select="$pagenum"/>
                </span>
                </span>
                </span>
              <!--<hr class="PRTPAGESUBLN"/>-->
            </span>
          </xsl:when>
          <xsl:otherwise>
          </xsl:otherwise>
        </xsl:choose>
      </xsl:when>
    </xsl:choose>
  </xsl:template>




 <!-- START GPOTABLES  -->

  <xsl:template match="GPOTABLE">
    <!--table style="table-layout:fixed;"-->
    <table style="table-layout:auto">
      <xsl:attribute name="class">
        <xsl:value-of select="name()"/>
      </xsl:attribute>
      <xsl:apply-templates/>
    </table>
  </xsl:template>

  <xsl:template match="TTITLE">
   <caption>
    <xsl:choose>
      <xsl:when test="not(node())">
      </xsl:when>
      <xsl:otherwise>
        <xsl:call-template name="apply-span"/>
      </xsl:otherwise>
    </xsl:choose>
   </caption>
  </xsl:template>

  <xsl:template match="BOXHD">
     <xsl:variable name="columns" select="(ancestor::GPOTABLE)[last()]/@COLS"/>
     <xsl:variable name="h2flag" select="count(child::CHED[@H=2])"/>
     <tr>
        <xsl:attribute name="colspan">
           <xsl:value-of select="$columns"/>
        </xsl:attribute>
        <xsl:attribute name="class">
           <xsl:value-of select="name()"/>
        </xsl:attribute>
           <xsl:call-template name="chedcounter"/>
      </tr>
        <xsl:if test="$h2flag">
           <tr>
              <xsl:call-template name="h2"/>
           </tr>
        </xsl:if>
  </xsl:template>

          <!-- To determine if current GPO table is a multi layer headers table -->
  <xsl:template name="chedcounter">
     <xsl:param name="node" select="." />
     <xsl:param name="node2" select="." />
     <xsl:param name="chc" select="0" />
     <xsl:param name="v" select="0" />

         <!-- Debugging section start -->
     <!--xsl:variable name="hvalue" select="CHED[$chc+1]/@H"/>
     <xsl:text>Here we are in CHEDCOUNTER = </xsl:text>
     <xsl:text>Here is node = </xsl:text><xsl:value-of select="$node"/>
     <xsl:text>Here is node2 = </xsl:text><xsl:value-of select="$node2"/>
     <xsl:text>Here is chc = </xsl:text><xsl:value-of select="$chc"/>
     <xsl:text>Here is v = </xsl:text><xsl:value-of select="$v"/-->
         <!-- Debugging section end -->

     <xsl:variable name="multiheader2sexist" select="count(child::CHED[@H=2])"/>
     <xsl:variable name="multiheader3sexist" select="count(child::CHED[@H=3])"/>
     <xsl:variable name="multiheader4sexist" select="count(child::CHED[@H=4])"/>
     <xsl:variable name="multiheader5sexist" select="count(child::CHED[@H=5])"/>

     <xsl:choose>
        <!--xsl:when test="$multiheader5sexist">
            <xsl:call-template name="maintemp2">
  	        <xsl:with-param name="node" select="$node2"/>
            </xsl:call-template>
        </xsl:when>
        <xsl:when test="$multiheader4sexist">
            <xsl:call-template name="maintemp2">
  	        <xsl:with-param name="node" select="$node2"/>
            </xsl:call-template>
        </xsl:when-->

        <xsl:when test="$multiheader3sexist">
            <xsl:call-template name="maintemp1">
  	        <xsl:with-param name="node" select="$node2"/>
            </xsl:call-template>
        </xsl:when>
        <xsl:when test="$multiheader2sexist">

               <xsl:call-template name="maintemp2">
  	          <xsl:with-param name="node" select="$node2"/>
               </xsl:call-template>

        </xsl:when>
        <xsl:when test="not($multiheader2sexist)">
            <xsl:call-template name="maintemp1">
  	        <xsl:with-param name="node" select="$node2"/>
            </xsl:call-template>
        </xsl:when>
     </xsl:choose>
  </xsl:template>

      <!-- Output a GPO table w just one layer of headers  -->
    <xsl:template name="maintemp1">
     <xsl:for-each select="CHED">
        <th>
           <xsl:attribute name="class">
              <xsl:text>GPOHEADERS </xsl:text>
              <xsl:value-of select="name()"/>
           </xsl:attribute>
           <xsl:apply-templates/>
        </th>
     </xsl:for-each>
  </xsl:template>


       <!-- Output a GPO table next layer of headers -->
  <xsl:template name="h2">
     <xsl:for-each select="CHED[@H=2]">
        <th class="GPOH2HEADERS"><xsl:apply-templates/></th>
     </xsl:for-each>
  </xsl:template>

       <!-- Output a GPO table w more than one layer of headers -->
  <!-- work in terms of position. keep position for h1, and accumulate h2 and call itself sending new values to variables,
  when h2 turns into h1 do output and reset variables by sending original values back-->

  <xsl:template name="maintemp2">
     <xsl:param name="node"/>
     <xsl:param name="nextnode"/>
     <xsl:param name="h1node"/>
     <xsl:param name="v"/>
     <xsl:param name="position" select="0"/>
     <xsl:param name="h1pos" select="1"/>
     <xsl:param name="h2pos" select="0"/>

     <xsl:if test="$node">
        <xsl:variable name="pos" select="$position + 1"/>
        <xsl:variable name="curnode" select="CHED[$pos]"/>
        <xsl:variable name="curhvalue" select="$curnode/@H"/>
        <xsl:variable name="nexthvalue" select="CHED[$pos+1]/@H"/>
        <xsl:variable name="nextnod" select="CHED[$pos+1]"/>

        <xsl:choose>
           <xsl:when test="($curhvalue = 1) and ($nexthvalue = 1)">
              <!-- do a rowspan = 2 -->
              <th class="GPOHEADERS">
	         <xsl:attribute name="rowspan">
		    <xsl:value-of select="2"/>
	         </xsl:attribute>
	         <!--xsl:attribute name="class">
		    <xsl:value-of select="name()"/>
	         </xsl:attribute>
	         <xsl:apply-templates/-->
	         <xsl:value-of select="$curnode"/>
              </th>
              <xsl:call-template name="maintemp2">
                 <xsl:with-param name="position" select="$pos"/>
                 <xsl:with-param name="node" select="$curnode"/>
                 <xsl:with-param name="h1node" select="$curnode"/>
                 <xsl:with-param name="v" select="$v"/>
                 <xsl:with-param name="h1pos" select="$pos"/>
  	       <xsl:with-param name="h2pos" select="$h2pos"/>
              </xsl:call-template>
           </xsl:when>

           <xsl:when test="($curhvalue = 1) and not($nexthvalue)">
              <!-- do a rowspan = 2 -->
              <th class="GPOHEADERS">
  	       <xsl:attribute name="rowspan">
  	          <xsl:value-of select="2"/>
  	       </xsl:attribute>
               <xsl:value-of select="$curnode"/>
              </th>
              <xsl:call-template name="maintemp2">
                 <xsl:with-param name="position" select="$pos"/>
                 <xsl:with-param name="node" select="0"/>
                 <xsl:with-param name="h1node" select="$curnode"/>
                 <xsl:with-param name="v" select="$v"/>
                 <xsl:with-param name="h1pos" select="$pos"/>
  	       <xsl:with-param name="h2pos" select="$h2pos"/>
              </xsl:call-template>
           </xsl:when>

           <xsl:when test="($curhvalue = 1) and ($nexthvalue = 2)">
              <!-- keep h1value and start accumulating h2value -->
              <xsl:call-template name="maintemp2">
                 <xsl:with-param name="position" select="$pos"/>
                 <xsl:with-param name="node" select="$curnode"/>
                 <xsl:with-param name="h1node" select="$curnode"/>
                 <xsl:with-param name="v" select="$v"/>
                 <xsl:with-param name="h1pos" select="$h1pos"/>
  	       <xsl:with-param name="h2pos" select="$h2pos+1"/>
              </xsl:call-template>
           </xsl:when>
           <xsl:when test="($curhvalue = 2) and ($nexthvalue = 2)">
              <!-- keep accumulating h2value and send also h1value -->
              <xsl:call-template name="maintemp2">
                 <xsl:with-param name="position" select="$pos"/>
                 <xsl:with-param name="node" select="$curnode"/>
                 <xsl:with-param name="h1node" select="$h1node"/>
                 <xsl:with-param name="v" select="$v"/>
                 <xsl:with-param name="h1pos" select="$h1pos"/>
  	       <xsl:with-param name="h2pos" select="$h2pos+1"/>
              </xsl:call-template>
           </xsl:when>

           <xsl:when test="($curhvalue = 2) and ($nexthvalue = 1)">
              <!-- do a colspan with h2value and reset all values again h1value and h2value -->
              <th class="GPOHEADERS">
                 <xsl:attribute name="colspan">
  	            <xsl:value-of select="$h2pos"/>
  	         </xsl:attribute>
  	         <xsl:value-of select="$h1node"/>
              </th>
              <xsl:call-template name="maintemp2">
                 <xsl:with-param name="position" select="$pos"/>
                 <xsl:with-param name="node" select="$curnode"/>
                 <xsl:with-param name="h1node" select="0"/>
                 <xsl:with-param name="v" select="$v"/>
                 <xsl:with-param name="h1pos" select="$h1pos"/>
  	       <xsl:with-param name="h2pos" select="0"/>
              </xsl:call-template>
           </xsl:when>

           <xsl:when test="($curhvalue = 2) and not($nexthvalue)">
              <!-- do a colspan with h2value and reset all values again h1value and h2value -->
              <th class="GPOHEADERS">
                 <xsl:attribute name="colspan">
  	            <xsl:value-of select="$h2pos"/>
  	         </xsl:attribute>
  	         <xsl:value-of select="$h1node"/>
              </th>
              <xsl:call-template name="maintemp2">
                 <xsl:with-param name="position" select="$pos"/>
                 <xsl:with-param name="node" select="0"/>
                 <xsl:with-param name="h1node" select="0"/>
                 <xsl:with-param name="v" select="$v"/>
                 <xsl:with-param name="h1pos" select="$h1pos"/>
  	       <xsl:with-param name="h2pos" select="0"/>
              </xsl:call-template>
           </xsl:when>
           <xsl:otherwise>
           </xsl:otherwise>
        </xsl:choose>
     </xsl:if>
  </xsl:template>

  <xsl:template match="ROW">
	  <xsl:variable name="expstb">
		<xsl:choose>
			<xsl:when test="./@EXPSTB[1]">
				<xsl:value-of select="./@EXPSTB[1]" />
			</xsl:when>
			<xsl:otherwise>
				<xsl:text>0</xsl:text>
			</xsl:otherwise>
		</xsl:choose>
	  </xsl:variable>
	  <!-- Needs to address @A="L01" -->
	  <xsl:variable name="NumOfENT" select="count(child::ENT) + sum(child::ENT/@A[number(.)=number(.)]) + $expstb"/>
      <xsl:variable name="columns" select="(ancestor::GPOTABLE)[last()]/@COLS"/>
      <xsl:variable name="total" select="$columns - $NumOfENT"/>
      <tr>
      <xsl:attribute name="class">
 		 <xsl:choose>
			 <xsl:when test="./@RUL='rn,s'">ROW-RUL-NSBAR </xsl:when>
			 <xsl:when test="./@RUL='n,s'">ROW-RUL-NSBAR </xsl:when>
			 <xsl:when test="./@RUL='s'">ROW-RUL-SBAR </xsl:when>
		 </xsl:choose>
         <xsl:value-of select="name()"/>
      </xsl:attribute>
      <!--xsl:attribute name="colspan">
         <xsl:value-of select="$columns"/>
      </xsl:attribute-->
      <xsl:for-each select="ENT">
         <xsl:call-template name="MyENT"/>
      </xsl:for-each>
      <xsl:choose>
         <xsl:when test="not($total = 0)">
            <xsl:call-template name="newENT">
               <xsl:with-param name="numofents" select="$total"/>
            </xsl:call-template>
         </xsl:when>
         <xsl:otherwise>
         </xsl:otherwise>
      </xsl:choose>
      </tr>
      <xsl:call-template name="apply-span"/>
  </xsl:template>

  <xsl:attribute-set name="td-list">
     <xsl:attribute name="class">MyENT ENT</xsl:attribute>
  </xsl:attribute-set>

  <xsl:template name="newENT">
     <xsl:param name="numofents"/>
     <xsl:if test="not($numofents = 0)">
        <xsl:element name="td" use-attribute-sets="td-list">
           <!-- xsl:text>N/A</xsl:text-->
        </xsl:element>
        <xsl:call-template name="newENT">
           <xsl:with-param name="numofents" select="$numofents - 1"/>
        </xsl:call-template>
     </xsl:if>
  </xsl:template>

  <xsl:template name="MyENT">
   <xsl:variable name="entValue" select="./@I"/>
    <td>
      <xsl:attribute name="class">
        <xsl:value-of select="name()"/>
      </xsl:attribute>
      <xsl:choose>
		<!-- Needs to also address @A="L01" -->
		<xsl:when test="./@A"><xsl:attribute name="colspan"><xsl:value-of select="./@A + 1"/></xsl:attribute><xsl:attribute name="style">text-align:center</xsl:attribute></xsl:when>
		<xsl:when test="../@EXPSTB and position()=1"><xsl:attribute name="colspan"><xsl:value-of select="../@EXPSTB + 1"/></xsl:attribute></xsl:when>
	  </xsl:choose>
      <xsl:choose>
          <xsl:when test="./@I=30">
             <a>
               <xsl:variable name="entVal" select=". mod 10"/>
               <xsl:variable name="ent" select="format-number($entVal,'0')" />
                <xsl:choose>
                 <xsl:when test="not($ent='NaN')">
                  <xsl:attribute name="href">
                     <xsl:text>#seqnum</xsl:text><xsl:value-of select="."/>
                  </xsl:attribute>
                  <xsl:apply-templates/>
                </xsl:when>
                <xsl:otherwise><xsl:value-of select="."/></xsl:otherwise>
               </xsl:choose>
             </a>
          </xsl:when>
          <xsl:when test="(preceding-sibling::ENT[@I=01] or preceding-sibling::ENT[@I=02]) and not($entValue) and (preceding::TTITLE/E[@T=12]='TABLE OF CONTENTS')">
	     <a>
	      <xsl:variable name="entVal" select=". mod 10"/>
	      <xsl:variable name="ent" select="format-number($entVal,'0')" />
              <xsl:choose>
               <xsl:when test="not($ent='NaN')">
	        <xsl:attribute name="href">
	          <xsl:text>#seqnum</xsl:text><xsl:value-of select="."/>
	        </xsl:attribute>
	        <xsl:apply-templates/>
	       </xsl:when>
	       <xsl:otherwise><xsl:value-of select="."/></xsl:otherwise>
	      </xsl:choose>
	     </a>
          </xsl:when>
        <xsl:otherwise>
          <xsl:apply-templates/>
        </xsl:otherwise>
      </xsl:choose>
    </td>
  </xsl:template>

  <xsl:template match="TNOTE | TDESC | SIGDAT">
     <tr>
        <td>
          <xsl:variable name="columns" select="(ancestor::GPOTABLE)[last()]/@COLS"/>
          <xsl:attribute name="colspan">
  	  <xsl:value-of select="$columns"/>
          </xsl:attribute>
          <xsl:attribute name="class">
            <xsl:value-of select="name()"/>
          </xsl:attribute>
          <xsl:apply-templates/>
        </td>
      </tr>
  </xsl:template>


<!-- END GPOTABLES  -->


  <xsl:template match="FP">
     <xsl:variable name="fpcontent1" select="substring(.,1,6)"/>
     <xsl:choose>
        <xsl:when test="$fpcontent1 = 'Email:'">
        <xsl:text>Email: </xsl:text>
        <xsl:variable name="fpcontent2" select="substring(.,8)"/>
        <a>
           <xsl:attribute name="href">
              <xsl:text>mailto:</xsl:text>
	      <xsl:value-of select="$fpcontent2"/>
           </xsl:attribute>
           <xsl:value-of select="$fpcontent2"/>
        </a>
        </xsl:when>
        <xsl:when test="./@SOURCE">
          <xsl:variable name="collapseSource" select="./@SOURCE"/>
          <p><span>
            <xsl:attribute name="class">
              <xsl:value-of select="name()"/>
              <xsl:text> </xsl:text>
              <xsl:value-of select="name(parent::*)"/>
              <xsl:text>-</xsl:text>
              <xsl:value-of select="$collapseSource"/>
            </xsl:attribute>
            <xsl:apply-templates/>
          </span></p>
        </xsl:when>
        <xsl:otherwise>
        <p><span>
          <xsl:attribute name="class">
            <xsl:value-of select="name()"/>
            <xsl:text> </xsl:text>
            <xsl:value-of select="name(parent::*)"/>
            <xsl:text>-</xsl:text>
            <xsl:value-of select="name()"/>
          </xsl:attribute>
          <xsl:apply-templates/>
        </span></p>
      </xsl:otherwise>
    </xsl:choose>

  </xsl:template>

  <!-- CFR PARTS AFFECTED -->

  <xsl:template match="AIDS/CFRPARTS/CFRS">
    <hr/>
    <div class="CPA">
    <div class="CPA-HED">CFR PARTS AFFECTED IN THIS ISSUE</div>
    <hr/>
    <p>A cumulative list of the parts affected this month can be found in the Reader Aids section at the end of this issue.</p>
    <table class="CPA-TABLE">
      <xsl:apply-templates/>
    </table>
    </div>
  </xsl:template>

  <xsl:template match="CFRS/CFR">
      <tr class="CPA-ROW-BOLD"><td><xsl:value-of select="."/></td><td/></tr>
  </xsl:template>

  <xsl:template match="CFRS/CFRPART">
    <tr>
      <td><xsl:value-of select="."/></td>
      <td><xsl:value-of select="following-sibling::FRPAGE[1]"/></td>
    </tr>
  </xsl:template>

  <xsl:template match="CFRS/FRPAGE"/>

  <xsl:template match="CFRS/PROCS">
      <td colspan="2" class="CPA-BOLD"><xsl:value-of select="HD"/></td>
      <xsl:apply-templates/>
  </xsl:template>

  <xsl:template match="PROCS/HD"/>

  <xsl:template match="CFRS/MOREPGS">
    <tr>
      <td/>
      <td><xsl:value-of select="."/></td>
    </tr>
  </xsl:template>

  <xsl:template match="PROCS/PROCNO">
    <tr>
      <td><xsl:value-of select="."/></td>
      <td><xsl:value-of select="following-sibling::FRPAGE[1]"/></td>
    </tr>
  </xsl:template>

  <xsl:template match="PROCS/FRPAGE"/>

  <xsl:template match="PROCS/MOREPGS">
    <tr>
      <td/>
      <td><xsl:value-of select="."/></td>
    </tr>
  </xsl:template>

  <xsl:template match="CFRS/PRORS">
    <td colspan="2" class="CPA-BOLD"><xsl:value-of select="HD"/></td>
    <xsl:apply-templates/>
  </xsl:template>

  <xsl:template match="PRORS/HD"/>

  <xsl:template match="PRORS/PRORNO">
    <tr>
      <td><xsl:value-of select="."/></td>
      <td><xsl:value-of select="following-sibling::FRPAGE[1]"/></td>
    </tr>
  </xsl:template>

  <xsl:template match="PRORS/FRPAGE"/>

  <xsl:template match="PRORS/MOREPGS">
    <tr>
      <td/>
      <td><xsl:value-of select="."/></td>
    </tr>
  </xsl:template>

  <!-- END CFR PARTS AFFECTED -->



  <xsl:template match="REGTEXT/AMDPAR">
    <xsl:call-template name="apply-span"/>
    <p/>
  </xsl:template>

  <xsl:template match="RULE/PREAMB/SUM">
    <hr/>
    <xsl:call-template name="apply-span"/>
  </xsl:template>

  <xsl:template match="E">
    <span>
       <xsl:attribute name="class">E-<xsl:value-of select="@T"/></xsl:attribute>
       <xsl:choose>
          <xsl:when test="./@T=03 and contains(.,'&#64;')">
	      <xsl:variable name="lastChar" select="substring(.,(string-length(.)),(string-length(.)))"/>
	      <xsl:variable name="InitChar" select="substring(.,1,8)"/>
	      <xsl:choose>
		 <xsl:when test="(($lastChar = '&#46;' or $lastChar = '&#44;') and ($InitChar = 'E-mail: ' or $InitChar = 'E-Mail: '))">
		    <xsl:variable name="email" select="substring(.,1,(string-length(.)-1))"/>
		    <xsl:value-of select="$InitChar"/>
		    <a><xsl:attribute name="href"><xsl:text>mailto:</xsl:text>
		       <xsl:value-of select="substring($email,9)"/>
		    </xsl:attribute><xsl:value-of select="substring($email,9)"/></a>
		    <xsl:value-of select="$lastChar"/>
		 </xsl:when>
		 <xsl:when test="$InitChar = 'E-mail: ' or $InitChar = 'E-Mail: '">
		    <xsl:value-of select="$InitChar"/>
		    <a><xsl:attribute name="href"><xsl:text>mailto:</xsl:text>
		       <xsl:value-of select="substring(.,9)"/>
		    </xsl:attribute><xsl:value-of select="substring(.,9)"/></a>
		 </xsl:when>
		 <xsl:when test="$lastChar = '&#46;' or $lastChar = '&#44;'">
		    <a><xsl:attribute name="href"><xsl:text>mailto:</xsl:text>
		       <xsl:value-of select="substring(.,1,(string-length(.)-1))"/>
		    </xsl:attribute><xsl:value-of select="substring(.,1,(string-length(.)-1))"/></a>
		    <xsl:value-of select="$lastChar"/>
		 </xsl:when>
		 <xsl:otherwise>
		    <a><xsl:attribute name="href"><xsl:text>mailto:</xsl:text>
		       <xsl:value-of select="."/>
		    </xsl:attribute><xsl:apply-templates/></a>
		 </xsl:otherwise>
	      </xsl:choose>
          </xsl:when>
          <xsl:otherwise>
             <xsl:apply-templates/>
          </xsl:otherwise>
       </xsl:choose>
    </span>
  </xsl:template>

  <xsl:template match="GPH/GID">
    <span class="GID">
      [Please see PDF for image:<xsl:text> </xsl:text><xsl:value-of select="."/>]
	  <!-- img class="GPH-GID" src="https://s3.amazonaws.com/images.federalregister.gov/{.}/original.png" height="{concat(../@DEEP, 'px')}"/ -->
    </span>
  </xsl:template>

  <xsl:template match="MATH/MID">
        <span class="MATH-MID">
          [Please see PDF for Formula:<xsl:text> </xsl:text><xsl:value-of select="."/>]
		  <!-- img class="MATH-MID" src="https://s3.amazonaws.com/images.federalregister.gov/{.}/original.png" height="{concat(../@DEEP, 'px')}"/ -->

        </span>
  </xsl:template>

  <xsl:template match="STARS">
    <span class="STARS">
      <xsl:text>* * * * *</xsl:text>
    </span>
  </xsl:template>

  <xsl:template match="HD">
    <xsl:choose>
      <xsl:when test="./@SOURCE">
        <xsl:variable name="collapseSource" select="./@SOURCE"/>
        <span>
          <xsl:attribute name="class">
            <xsl:value-of select="name()"/>
            <xsl:text> </xsl:text>
            <xsl:value-of select="name(parent::*)"/>
            <xsl:text>-</xsl:text>
            <xsl:value-of select="$collapseSource"/>
          </xsl:attribute>
          <xsl:apply-templates/>
        </span>
      </xsl:when>
      <xsl:otherwise>
        <span>
          <xsl:attribute name="class">
            <xsl:value-of select="name()"/>
            <xsl:text> </xsl:text>
            <xsl:value-of select="name(parent::*)"/>
            <xsl:text>-</xsl:text>
            <xsl:value-of select="name()"/>
          </xsl:attribute>
          <xsl:apply-templates/>
        </span>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template match="P">
    <xsl:variable name="precedingSib" select="(preceding-sibling::*)[last()]/@SOURCE"/>
    <xsl:variable name="precedingSibTag" select="name(preceding-sibling::*[1])"/>
      <xsl:choose>
      <xsl:when test="$precedingSib='HED' and ancestor::UNIFIED=false">
        <span><xsl:attribute name="class"><xsl:value-of select="name(parent::*)"/><xsl:value-of select="name()"/><xsl:text> </xsl:text><xsl:value-of select="$precedingSib"/>-P</xsl:attribute>
          <xsl:apply-templates/>
          <xsl:if test="not(name(parent::*)='SEE')"><p/></xsl:if>
        </span>
      </xsl:when>
      <xsl:when test="$precedingSib='HD1' and ancestor::UNIFIED=false">
        <span>
          <xsl:attribute name="class">
            <xsl:value-of select="name(parent::*)"/>
            <xsl:value-of select="name()"/>
            <xsl:text> </xsl:text>
            <xsl:value-of select="$precedingSib"/>-P</xsl:attribute>
          <xsl:apply-templates/><p/>
        </span>
      </xsl:when>
      <xsl:when test="ancestor::UNIFIED">
        <span>
          <xsl:attribute name="class">UA-<xsl:value-of select="name()"/> UA-<xsl:value-of select="$precedingSibTag"/>-PADDING</xsl:attribute>
          <xsl:apply-templates/>
        </span>
      </xsl:when>
      <xsl:otherwise>
        <span class="P">
          <xsl:apply-templates/><p/>
        </span>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template match="COMPLETD">
     <xsl:variable name="trEnd">&lt;/tr></xsl:variable>
     <xsl:choose>
       <xsl:when test="ancestor::UNIFIED">
          <table>
            <xsl:attribute name="class">UA-<xsl:value-of select="name()"/></xsl:attribute>
            <xsl:apply-templates/>
            <xsl:value-of select="$trEnd" disable-output-escaping="yes"/>
          </table>
       </xsl:when>
       <xsl:otherwise>
         <xsl:call-template name="apply-span"/>
       </xsl:otherwise>
     </xsl:choose>
  </xsl:template>

  <xsl:template match="COMPLETD/HED | COMPLETD/REASON | COMPLETD/DATE | COMPLETD/CITA | COMPLETD/ACTION">
      <xsl:variable name="thisNode" select="."/>
      <xsl:variable name="thisNodeTag" select="name()"/>
      <xsl:variable name="precedingSibTag" select="name(preceding-sibling::*[1])"/>
      <xsl:variable name="countFromHED" select="count(preceding-sibling::*[name()!='PRTPAGE'])"/>
      <xsl:variable name="trBegin">&lt;tr></xsl:variable>
      <xsl:variable name="trEnd">&lt;/tr></xsl:variable>
      <xsl:choose>
        <xsl:when test="$thisNodeTag='HED'">
          <caption><xsl:value-of select="$thisNode"/></caption>
        </xsl:when>
        <xsl:when test="$thisNodeTag='REASON' and $countFromHED&lt;4">
          <xsl:value-of select="$trBegin" disable-output-escaping="yes"/>
          <th class="UA-COL-ACTION"><xsl:value-of select="$thisNode"/></th>
        </xsl:when>
        <xsl:when test="$thisNodeTag='DATE' and $countFromHED&lt;4">
          <th class="UA-COL-DATE"><xsl:value-of select="$thisNode"/></th>
        </xsl:when>
        <xsl:when test="$thisNodeTag='CITA' and $countFromHED&lt;4">
          <th class="UA-COL-CITA"><xsl:value-of select="$thisNode"/></th>
        </xsl:when>
        <xsl:when test="$thisNodeTag='ACTION' and $countFromHED&gt;3">
          <xsl:value-of select="$trEnd" disable-output-escaping="yes"/>
          <xsl:value-of select="$trBegin" disable-output-escaping="yes"/>
          <td class="UA-COL-ACTION"><xsl:value-of select="$thisNode"/></td>
        </xsl:when>
        <xsl:when test="$thisNodeTag='DATE' and $countFromHED&gt;3">
          <td class="UA-COL-DATE"><xsl:value-of select="$thisNode"/></td>
        </xsl:when>
        <xsl:when test="$thisNodeTag='CITA' and $countFromHED&gt;3">
          <xsl:choose>
            <xsl:when test="$precedingSibTag!='DATE'">
                <td class="UA-COL-DATE"> </td>
            </xsl:when>
          </xsl:choose>
          <td class="UA-COL-CITA"><xsl:value-of select="$thisNode"/></td>
        </xsl:when>
        <xsl:otherwise>
          <xsl:call-template name="apply-span"/>
        </xsl:otherwise>
     </xsl:choose>
  </xsl:template>


  <xsl:template match="TIMETBL">
     <xsl:variable name="trEnd">&lt;/tr></xsl:variable>
     <xsl:choose>
       <xsl:when test="ancestor::UNIFIED">
          <table>
            <xsl:attribute name="class">UA-<xsl:value-of select="name()"/></xsl:attribute>
            <xsl:apply-templates/>
            <xsl:value-of select="$trEnd" disable-output-escaping="yes"/>
          </table>
       </xsl:when>
       <xsl:otherwise>
         <xsl:call-template name="apply-span"/>
       </xsl:otherwise>
     </xsl:choose>
  </xsl:template>

  <xsl:template match="TIMETBL/HED | TIMETBL/REASON | TIMETBL/DATE | TIMETBL/CITA | TIMETBL/ACTION">
      <xsl:variable name="thisNode" select="."/>
      <xsl:variable name="thisNodeTag" select="name()"/>
      <xsl:variable name="precedingSibTag" select="name(preceding-sibling::*[1])"/>
      <xsl:variable name="countFromHED" select="count(preceding-sibling::*[name()!='PRTPAGE'])"/>
      <xsl:variable name="trBegin">&lt;tr></xsl:variable>
      <xsl:variable name="trEnd">&lt;/tr></xsl:variable>
      <xsl:choose>
        <xsl:when test="$thisNodeTag='HED'">
          <caption><xsl:value-of select="$thisNode"/></caption>
        </xsl:when>
        <xsl:when test="$thisNodeTag='ACTION' and $countFromHED&lt;4">
          <xsl:value-of select="$trBegin" disable-output-escaping="yes"/>
          <th class="UA-COL-ACTION"><xsl:value-of select="$thisNode"/></th>
        </xsl:when>
        <xsl:when test="$thisNodeTag='DATE' and $countFromHED&lt;4">
          <th class="UA-COL-DATE"><xsl:value-of select="$thisNode"/></th>
        </xsl:when>
        <xsl:when test="$thisNodeTag='CITA' and $countFromHED&lt;4">
          <th class="UA-COL-CITA"><xsl:value-of select="$thisNode"/></th>
        </xsl:when>
        <xsl:when test="$thisNodeTag='ACTION' and $countFromHED&gt;3">
          <xsl:value-of select="$trEnd" disable-output-escaping="yes"/>
          <xsl:value-of select="$trBegin" disable-output-escaping="yes"/>
          <td class="UA-COL-ACTION"><xsl:value-of select="$thisNode"/></td>
        </xsl:when>
        <xsl:when test="$thisNodeTag='DATE' and $countFromHED&gt;3">
          <td class="UA-COL-DATE"><xsl:value-of select="$thisNode"/></td>
        </xsl:when>
        <xsl:when test="$thisNodeTag='CITA' and $countFromHED&gt;3">
          <xsl:choose>
            <xsl:when test="$precedingSibTag!='DATE'">
                <td class="UA-COL-DATE"> </td>
            </xsl:when>
          </xsl:choose>
          <td class="UA-COL-CITA"><xsl:value-of select="$thisNode"/></td>
        </xsl:when>
        <xsl:otherwise>
          <xsl:call-template name="apply-span"/>
        </xsl:otherwise>
     </xsl:choose>
  </xsl:template>


    <!-- Anchor template section -->

      <xsl:template match="PGS">
       <xsl:variable name="pgsnum" select="substring-before(.,'-')"/>
        <span>
          <xsl:attribute name="class">
            <xsl:value-of select="name()"/>
            <xsl:text> </xsl:text>
            <xsl:value-of select="name(parent::*)"/>-<xsl:value-of select="name()"/>
          </xsl:attribute>
          <a>
            <xsl:attribute name="href">
               <xsl:choose>
                  <xsl:when test="$pgsnum">
                     <xsl:text>#seqnum</xsl:text><xsl:value-of select="$pgsnum"/>
                  </xsl:when>
                  <xsl:otherwise>
                     <!--xsl:variable name="pgsnum" select="./"/-->
		     <xsl:text>#seqnum</xsl:text><xsl:value-of select="."/>
                  </xsl:otherwise>
               </xsl:choose>
            </xsl:attribute>
            <xsl:apply-templates/>
          </a>
        </span>
      </xsl:template>


<!-- END Anchor template section -->



  <!-- Default Template Handling -->
  <xsl:template match="*" priority="-10">
    <xsl:choose>
      <xsl:when test="not(node())">
        <!--  DEBUG: Enable to detect empty tags.
        <span>
          [EMPTY-NODE <xsl:value-of select="name()"/>]
        </span>
        -->
      </xsl:when>
      <xsl:otherwise>
        <xsl:call-template name="apply-span"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template name="apply-span">
    <span>
      <xsl:attribute name="class">
        <xsl:choose>
          <xsl:when test="ancestor::UNIFIED">UA-<xsl:value-of select="name()"/><xsl:text> UA-</xsl:text><xsl:value-of select="name(parent::*)"/>-<xsl:value-of select="name()"/></xsl:when>
          <xsl:otherwise><xsl:value-of select="name()"/><xsl:text> </xsl:text><xsl:value-of select="name(parent::*)"/>-<xsl:value-of select="name()"/></xsl:otherwise>
        </xsl:choose>
      </xsl:attribute>
      <xsl:apply-templates/>
    </span>
  </xsl:template>

</xsl:stylesheet>
