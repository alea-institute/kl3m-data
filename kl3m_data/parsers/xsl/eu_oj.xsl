<xsl:stylesheet version="1.0"
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns="http://www.w3.org/1999/xhtml"
    xmlns:fmx="http://opoce"
    exclude-result-prefixes="fmx">

  <xsl:output method="html" encoding="UTF-8" indent="yes" doctype-system="about:legacy-compat"/>
  <xsl:strip-space elements="*"/>

  <!-- Root template: Dispatch based on document type -->
  <xsl:template match="/">
    <html lang="en">
      <head>
        <meta charset="UTF-8"/>
        <!-- Extract a plain text title from the first TITLE/P found -->
        <title>
          <xsl:value-of select="//*[local-name()='TITLE'][1]//P[1]"/>
        </title>
        <style>
          /* General Document Styles */
          body { font-family: sans-serif; line-height: 1.6; margin: 2em; color: #333; }
          h1, h2, h3, h4 { color: #0056b3; }
          p { margin-bottom: 0.8em; }
          ul, ol { margin-bottom: 1em; }
          li { margin-bottom: 0.3em; }
          table { border-collapse: collapse; width: 100%; margin-bottom: 1em; }
          th, td { border: 1px solid #ccc; padding: 0.5em; text-align: left; }
          th { background-color: #f0f0f0; font-weight: bold; }
          time, em { font-style: italic; }
          pre { background-color: #f0f0f0; padding: 1em; border: 1px solid #ccc; overflow-x: auto; }
          code { font-family: monospace; background-color: #f0f0f0; padding: 0.2em; border: 1px solid #ddd; }
          blockquote { margin: 1em 0; padding-left: 1em; border-left: 3px solid #0056b3; font-style: italic; color: #555; }

          /* Metadata and structural styles */
          .metadata { border: 1px solid #ccc; padding: 1em; margin-bottom: 1em; background-color: #f9f9f9; }
          .metadata h2 { font-size: 1.2em; margin-top: 0; }
          .metadata ul { list-style: none; padding-left: 0; }
          .metadata li { margin-bottom: 0.5em; }
          .metadata strong { font-weight: bold; }
          .doc, .general, .cjt, .publication { border: 1px solid #aaa; padding: 1em; margin-bottom: 1em; }
          .doc h1, .general h1, .cjt h1, .publication h1 { font-size: 1.5em; margin-top: 0; }
          .section-title { font-weight: bold; margin-top: 1em; margin-bottom: 0.5em; }
          .subsection-title { font-style: italic; margin-top: 1em; margin-bottom: 0.5em; }
          .doc-ref, .oj-ref { font-style: italic; }
        </style>
      </head>
      <body>
        <xsl:choose>
          <xsl:when test="DOC">
            <xsl:apply-templates select="DOC"/>
          </xsl:when>
          <xsl:when test="GENERAL">
            <xsl:apply-templates select="GENERAL"/>
          </xsl:when>
          <xsl:when test="CJT">
            <xsl:apply-templates select="CJT"/>
          </xsl:when>
          <xsl:when test="PUBLICATION">
            <xsl:apply-templates select="PUBLICATION"/>
          </xsl:when>
          <xsl:otherwise>
            <p>Unknown document type.</p>
          </xsl:otherwise>
        </xsl:choose>
      </body>
    </html>
  </xsl:template>

  <!-- Templates for Document Roots -->
  <xsl:template match="DOC">
    <div class="doc">
      <xsl:apply-templates/>
    </div>
  </xsl:template>

  <xsl:template match="GENERAL">
    <div class="general">
      <xsl:apply-templates/>
    </div>
  </xsl:template>

  <xsl:template match="CJT">
    <div class="cjt">
      <xsl:apply-templates/>
    </div>
  </xsl:template>

  <xsl:template match="PUBLICATION">
    <div class="publication">
      <xsl:apply-templates/>
    </div>
  </xsl:template>

  <!-- Metadata Templates using mode "metadata" -->
  <xsl:template name="metadata-section">
    <div class="metadata">
      <h2><xsl:value-of select="name()"/></h2>
      <ul>
        <xsl:apply-templates select="*" mode="metadata"/>
      </ul>
    </div>
  </xsl:template>

  <xsl:template match="BIB.INSTANCE | BIB.DOC | PUBLICATION.REF | BIB.OJ | FMX | PAPER | PDF" mode="common-metadata">
    <xsl:call-template name="metadata-section"/>
  </xsl:template>

  <xsl:template match="*" mode="metadata">
    <li>
      <strong><xsl:value-of select="name()"/>:</strong>
      <xsl:choose>
        <xsl:when test="*">
          <ul>
            <xsl:apply-templates mode="metadata"/>
          </ul>
        </xsl:when>
        <xsl:otherwise>
          <xsl:value-of select="."/>
        </xsl:otherwise>
      </xsl:choose>
    </li>
  </xsl:template>

  <!-- TITLE Templates -->
  <xsl:template match="DOC/PAPER/VOLUME.PAPER/ITEM.VOLUME/TITLE">
    <xsl:apply-templates select="TI/P"/>
  </xsl:template>
  <xsl:template match="DOC/PDF/ITEM.VOLUME/TITLE"/>
  <xsl:template match="DOC/PAPER/VOLUME.PAPER/ITEM.VOLUME/TITLE[@ID.TITLE='T0001']/TI/P">
    <h2><xsl:value-of select="."/></h2>
  </xsl:template>
  <xsl:template match="GENERAL/TITLE">
    <h1><xsl:apply-templates select="TI/P"/></h1>
  </xsl:template>
  <xsl:template match="CJT/TI.CJT/TITLE">
    <h1><xsl:apply-templates select="TI/*"/></h1>
  </xsl:template>
  <xsl:template match="PUBLICATION/OJ/VOLUME/SECTION/TITLE">
    <h2><xsl:apply-templates select="TI/NP/TXT"/></h2>
  </xsl:template>
  <xsl:template match="PUBLICATION/OJ/VOLUME/SECTION/SUBSECTION/TITLE">
    <h3><xsl:apply-templates select="TI/P"/></h3>
  </xsl:template>
  <xsl:template match="PUBLICATION/OJ/VOLUME/SECTION/SUBSECTION/SUBSECTION/TITLE">
    <h4><xsl:apply-templates select="TI/P"/></h4>
  </xsl:template>
  <xsl:template match="CJT/TI.CJT/TITLE/TI/P">
    <h1><xsl:value-of select="."/></h1>
  </xsl:template>

  <!-- CONTENTS Templates -->
  <xsl:template match="GENERAL/CONTENTS">
    <section class="contents">
      <h2 class="section-title">Contents</h2>
      <xsl:apply-templates/>
    </section>
  </xsl:template>
  <xsl:template match="CJT/TI.CJT/CONTENTS">
    <section class="contents">
      <h2 class="section-title">Contents</h2>
      <xsl:apply-templates/>
    </section>
  </xsl:template>

  <!-- ENACTING.TERMS.CJT Template -->
  <xsl:template match="CJT/ENACTING.TERMS.CJT">
    <section class="enacting-terms-cjt">
      <h2 class="section-title">Enacting Terms</h2>
      <xsl:apply-templates/>
    </section>
  </xsl:template>


  <!-- INDEX Template -->
  <xsl:template match="CJT/TI.CJT/INDEX">
    <section class="index">
      <h2 class="section-title">Index</h2>
      <ul>
        <xsl:apply-templates/>
      </ul>
    </section>
  </xsl:template>
  <xsl:template match="CJT/TI.CJT/INDEX/KEYWORD">
    <li><xsl:value-of select="."/></li>
  </xsl:template>

  <!-- LG.PROC and TRANS.REF Templates -->
  <xsl:template match="CJT/TI.CJT/LG.PROC">
    <p><xsl:value-of select="."/></p>
  </xsl:template>
  <xsl:template match="CJT/TI.CJT/TRANS.REF">
    <p><strong>Translation Reference:</strong> <xsl:value-of select="."/></p>
  </xsl:template>

  <!-- GR.SEQ Template -->
  <xsl:template match="GR.SEQ">
    <section class="gr-seq">
      <h3 class="subsection-title">
        <xsl:apply-templates select="TITLE/TI/P"/>
      </h3>
      <xsl:apply-templates select="P|LIST|TBL"/>
    </section>
  </xsl:template>

  <!-- Table Templates -->
  <xsl:template match="TBL">
    <table>
      <xsl:apply-templates/>
    </table>
  </xsl:template>
  <xsl:template match="ROW">
    <tr>
      <xsl:apply-templates/>
    </tr>
  </xsl:template>
  <xsl:template match="CELL">
    <td>
      <xsl:apply-templates/>
    </td>
  </xsl:template>

  <!-- ITEM.VOLUME Templates -->
  <xsl:template match="PAPER/VOLUME.PAPER/ITEM.VOLUME">
    <section class="item-volume">
      <xsl:apply-templates select="TITLE"/>
      <p>Item Ref: <xsl:value-of select="ITEM.REF"/></p>
    </section>
  </xsl:template>
  <xsl:template match="PDF/ITEM.VOLUME">
    <section class="item-volume">
      <p>Item Ref (PDF): <xsl:value-of select="ITEM.REF/@REF.PDF"/></p>
      <p>Page: <xsl:value-of select="ITEM.REF"/></p>
    </section>
  </xsl:template>

  <!-- ITEM.PUB Templates in PUBLICATION -->
  <xsl:template match="PUBLICATION/OJ/VOLUME/SECTION/SUBSECTION/ITEM.PUB">
    <div class="item-pub">
      <xsl:apply-templates/>
    </div>
  </xsl:template>

  <!-- QUOTATION Handling -->
  <xsl:template match="QUOT.START[@CODE='2018']">
    <xsl:text>“</xsl:text>
  </xsl:template>
  <xsl:template match="QUOT.END[@CODE='2019']">
    <xsl:text>”</xsl:text>
  </xsl:template>
  <xsl:template match="QUOT.START[@CODE='201C']">
    <xsl:text>‘</xsl:text>
  </xsl:template>
  <xsl:template match="QUOT.END[@CODE='201D']">
    <xsl:text>’</xsl:text>
  </xsl:template>

  <!-- Generic P Template -->
  <xsl:template match="P">
    <p>
      <xsl:apply-templates/>
    </p>
  </xsl:template>

  <!-- Generic TXT Template -->
  <xsl:template match="TXT">
    <xsl:apply-templates/>
  </xsl:template>

  <!-- Specific P Template inside TI -->
  <xsl:template match="TI/P">
    <p>
      <xsl:apply-templates/>
    </p>
  </xsl:template>

  <!-- NO.DOC.C and NO.DOC.OJ Templates -->
  <xsl:template match="NO.DOC.C">
    <p class="doc-ref">Case No: <xsl:value-of select="."/></p>
  </xsl:template>
  <xsl:template match="NO.DOC.OJ">
    <p class="oj-ref">OJ No: <xsl:value-of select="TXT"/></p>
  </xsl:template>

  <!-- REF.DOC.OJ Template -->
  <xsl:template match="REF.DOC.OJ">
    <xsl:variable name="oj_ref">
      <xsl:text>OJ </xsl:text>
      <xsl:value-of select="@COLL"/>
      <xsl:text> </xsl:text>
      <xsl:value-of select="@NO.OJ"/>
      <xsl:text> of </xsl:text>
      <xsl:value-of select="@DATE.PUB"/>
    </xsl:variable>
    <cite>
      <xsl:value-of select="$oj_ref"/>
    </cite>
  </xsl:template>

  <!-- NP/TXT Template -->
  <xsl:template match="NP/TXT">
    <p>
      <xsl:value-of select="../NO.P"/>
      <xsl:text> </xsl:text>
      <xsl:apply-templates/>
    </p>
  </xsl:template>

  <!-- Handle IE elements (empty) -->
  <xsl:template match="IE"/>

  <!-- Handle FT elements -->
  <xsl:template match="FT">
    <xsl:value-of select="."/>
  </xsl:template>

</xsl:stylesheet>
