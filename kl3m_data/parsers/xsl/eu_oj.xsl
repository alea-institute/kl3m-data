<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0"
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns="http://www.w3.org/1999/xhtml"
    xmlns:fmx="http://opoce"
    exclude-result-prefixes="fmx">

  <xsl:output method="html" encoding="UTF-8" indent="yes" doctype-system="about:legacy-compat"/>
  <xsl:strip-space elements="*"/>

  <!-- Root template -->
  <xsl:template match="/">
    <html lang="en">
      <head>
        <meta charset="UTF-8"/>
        <title>
          <xsl:value-of select="//*[local-name()='TITLE'][1]//P[1]"/>
        </title>
        <style>
          /* General Document Styles */
          body {
            font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            line-height: 1.6;
            margin: 2em;
            color: #333;
            max-width: 1200px;
            margin: 0 auto;
            padding: 2em;
          }

          /* Document Header Styles */
          .document-header {
            margin-bottom: 2em;
            border-bottom: 2px solid #0056b3;
            padding-bottom: 1em;
          }

          .title-content {
            margin-bottom: 1.5em;
          }

          .document-number {
            font-size: 1.1em;
            color: #0056b3;
            margin-bottom: 1em;
          }

          .title-paragraph {
            margin: 0.5em 0;
            line-height: 1.4;
            font-size: 1.1em;
          }

          .date-container {
            color: #666;
            font-style: italic;
            margin: 0.5em 0;
          }

          .document-identifier {
            font-weight: 500;
            color: #333;
            margin-bottom: 1em;
          }

          .document-metadata {
            margin-bottom: 2em;
            padding: 1em;
            background-color: #f8f9fa;
            border-left: 3px solid #0056b3;
          }

          /* Content Section Styles */
          .content-section {
            margin: 1.5em 0;
            padding: 1em;
          }

          .content-title {
            margin-bottom: 1em;
          }

          .content-subject {
            display: flex;
            gap: 0.5em;
            margin-bottom: 0.5em;
          }

          .content-subject em {
            color: #666;
            min-width: 80px;
          }

          .subject-text {
            font-weight: 500;
          }

          /* Numbered Paragraphs */
          .numbered-paragraph {
            display: flex;
            gap: 1em;
            margin: 1em 0;
            align-items: flex-start;
          }

          .paragraph-number {
            flex-shrink: 0;
            min-width: 2em;
            font-weight: 500;
            color: #0056b3;
          }

          .paragraph-text {
            flex: 1;
            line-height: 1.6;
          }

          /* Lists */
          .alpha-list {
            list-style-type: lower-alpha;
            margin: 1em 0 1em 3em;
            padding-left: 1em;
          }

          .alpha-list li {
            margin-bottom: 0.8em;
            padding-left: 0.5em;
          }

          /* Notes and References */
          .note {
            margin: 1em 0;
            padding: 1em;
            background-color: #f8f9fa;
            border-left: 3px solid #ffc107;
          }

          .document-reference {
            font-style: italic;
            color: #0056b3;
          }

          /* Tables */
          table {
            border-collapse: collapse;
            width: 100%;
            margin: 1em 0;
          }

          th, td {
            border: 1px solid #ddd;
            padding: 0.8em;
            text-align: left;
          }

          th {
            background-color: #f8f9fa;
            font-weight: 500;
          }
        </style>
      </head>
      <body>
        <xsl:apply-templates/>
      </body>
    </html>
  </xsl:template>

  <!-- Document Structure Templates -->
  <xsl:template match="GENERAL|DOC|CJT|PUBLICATION">
    <div class="document">
      <xsl:apply-templates/>
    </div>
  </xsl:template>

  <!-- BIB.INSTANCE metadata -->
  <xsl:template match="BIB.INSTANCE">
    <div class="document-metadata">
      <xsl:apply-templates select="DOCUMENT.REF"/>
      <xsl:apply-templates select="NO.DOC"/>
      <xsl:apply-templates select="*[not(self::DOCUMENT.REF|self::NO.DOC)]"/>
    </div>
  </xsl:template>

  <!-- Title Section Templates -->
  <xsl:template match="TITLE">
    <header class="document-header">
      <xsl:apply-templates/>
    </header>
  </xsl:template>

  <xsl:template match="TI">
    <div class="title-content">
      <xsl:apply-templates/>
    </div>
  </xsl:template>

  <xsl:template match="NO.DOC.C">
    <p class="document-number">
      <xsl:value-of select="."/>
    </p>
  </xsl:template>

  <xsl:template match="TI/P">
    <p class="title-paragraph">
      <xsl:apply-templates/>
    </p>
  </xsl:template>

  <!-- Content Section Templates -->
  <xsl:template match="CONTENTS">
    <main class="document-contents">
      <xsl:apply-templates/>
    </main>
  </xsl:template>

  <xsl:template match="GR.SEQ">
    <section class="content-section">
      <xsl:apply-templates/>
    </section>
  </xsl:template>

  <!-- Numbered Paragraphs -->
  <xsl:template match="NP">
    <div class="numbered-paragraph">
      <xsl:if test="NO.P">
        <span class="paragraph-number">
          <xsl:value-of select="NO.P"/>
        </span>
      </xsl:if>
      <div class="paragraph-text">
        <xsl:apply-templates select="TXT|P|LIST"/>
      </div>
    </div>
  </xsl:template>

  <!-- Lists -->
  <xsl:template match="LIST">
    <xsl:choose>
      <xsl:when test="@TYPE='alpha'">
        <ol class="alpha-list">
          <xsl:apply-templates/>
        </ol>
      </xsl:when>
      <xsl:otherwise>
        <ul>
          <xsl:apply-templates/>
        </ul>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template match="ITEM">
    <li>
      <xsl:apply-templates/>
    </li>
  </xsl:template>

  <!-- Text and Paragraph Elements -->
  <xsl:template match="TXT">
    <xsl:apply-templates/>
  </xsl:template>

  <xsl:template match="P">
    <p>
      <xsl:apply-templates/>
    </p>
  </xsl:template>

  <!-- Date Handling -->
  <xsl:template match="DATE">
    <time datetime="{@ISO}">
      <xsl:value-of select="."/>
    </time>
  </xsl:template>

  <!-- Notes and References -->
  <xsl:template match="NOTE">
    <span id="note-{@NOTE.ID}">
      <xsl:apply-templates/>
    </span>
  </xsl:template>

  <xsl:template match="REF.DOC.OJ">
    <span class="document-reference">
      <xsl:value-of select="@COLL"/>
      <xsl:text> </xsl:text>
      <xsl:value-of select="@NO.OJ"/>
      <xsl:text>, </xsl:text>
      <xsl:value-of select="@DATE.PUB"/>
      <xsl:if test="@PAGE.FIRST">
        <xsl:text>, p. </xsl:text>
        <xsl:value-of select="@PAGE.FIRST"/>
      </xsl:if>
    </span>
  </xsl:template>

  <!-- Table Handling -->
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

  <!-- Special Character Handling -->
  <xsl:template match="QUOT.START[@CODE='2018']">
    <xsl:text>'</xsl:text>
  </xsl:template>

  <xsl:template match="QUOT.END[@CODE='2019']">
    <xsl:text>'</xsl:text>
  </xsl:template>

  <xsl:template match="QUOT.START[@CODE='201C']">
    <xsl:text>"</xsl:text>
  </xsl:template>

  <xsl:template match="QUOT.END[@CODE='201D']">
    <xsl:text>"</xsl:text>
  </xsl:template>

</xsl:stylesheet>