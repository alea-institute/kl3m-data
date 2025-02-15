<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0"
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns="http://www.w3.org/1999/xhtml">

  <xsl:output method="html" encoding="UTF-8" indent="yes" doctype-system="about:legacy-compat"/>

  <!-- Root template: start from the ACT element -->
  <xsl:template match="/">
    <html lang="en">
      <head>
        <meta charset="UTF-8"/>
        <title>
          <xsl:value-of select="/ACT/TITLE/TI/P[1]"/>
        </title>
      </head>
      <body>
        <xsl:apply-templates select="/ACT/*"/>
      </body>
    </html>
  </xsl:template>

  <!-- ACT element wrapper -->
  <xsl:template match="ACT">
    <div class="act">
      <xsl:apply-templates/>
    </div>
  </xsl:template>

  <!-- Metadata: BIB.INSTANCE, BIB.DOC, PUBLICATION.REF, BIB.OJ -->
  <xsl:template match="BIB.INSTANCE | BIB.DOC | PUBLICATION.REF | BIB.OJ">
    <div class="metadata">
      <h2>Metadata</h2>
      <ul>
        <xsl:apply-templates select="*" mode="metadata"/>
      </ul>
    </div>
  </xsl:template>

  <xsl:template match="*" mode="metadata">
    <li>
      <strong><xsl:value-of select="name()"/>: </strong>
      <xsl:value-of select="."/>
    </li>
  </xsl:template>

  <!-- TITLE: Render the main title -->
  <xsl:template match="TITLE">
    <header>
      <h1>
        <xsl:apply-templates select="TI/P"/>
      </h1>
    </header>
  </xsl:template>

  <!-- PREAMBLE -->
  <xsl:template match="PREAMBLE">
    <section class="preamble">
      <h2>Preamble</h2>
      <xsl:apply-templates/>
    </section>
  </xsl:template>

  <!-- ENACTING TERMS -->
  <xsl:template match="ENACTING.TERMS">
    <section class="enacting-terms">
      <h2>Enacting Terms</h2>
      <xsl:apply-templates/>
    </section>
  </xsl:template>

  <!-- FINAL provisions -->
  <xsl:template match="FINAL">
    <footer>
      <xsl:apply-templates/>
    </footer>
  </xsl:template>

  <!-- DIVISION (e.g. Chapters) -->
  <xsl:template match="DIVISION">
    <div class="division">
      <xsl:apply-templates select="TITLE"/>
      <xsl:apply-templates select="*"/>
    </div>
  </xsl:template>

  <!-- ARTICLE elements -->
  <xsl:template match="ARTICLE">
    <article>
      <header>
        <xsl:apply-templates select="TI.ART"/>
        <xsl:apply-templates select="STI.ART"/>
      </header>
      <xsl:apply-templates/>
    </article>
  </xsl:template>

  <!-- PARAG: paragraphs within articles -->
  <xsl:template match="PARAG">
    <p>
      <xsl:apply-templates/>
    </p>
  </xsl:template>

  <!-- ALINEA: treat as paragraphs -->
  <xsl:template match="ALINEA">
    <p>
      <xsl:apply-templates/>
    </p>
  </xsl:template>

  <!-- NP: also output as paragraph -->
  <xsl:template match="NP">
    <p>
      <xsl:apply-templates/>
    </p>
  </xsl:template>

  <!-- P: generic paragraph -->
  <xsl:template match="P">
    <p>
      <xsl:apply-templates/>
    </p>
  </xsl:template>

  <!-- LIST and ITEM for lists -->
  <xsl:template match="LIST">
    <ul>
      <xsl:apply-templates/>
    </ul>
  </xsl:template>

  <xsl:template match="ITEM">
    <li>
      <xsl:apply-templates/>
    </li>
  </xsl:template>

  <!-- DATE: output a time element -->
  <xsl:template match="DATE">
    <time datetime="{@ISO}">
      <xsl:value-of select="."/>
    </time>
  </xsl:template>

  <!-- HT: Formatting helpers -->
  <xsl:template match="HT[@TYPE='ITALIC']">
    <em>
      <xsl:apply-templates/>
    </em>
  </xsl:template>

  <xsl:template match="HT[@TYPE='UC']">
    <span style="text-transform: uppercase;">
      <xsl:apply-templates/>
    </span>
  </xsl:template>

  <!-- NOTE: Footnotes or side notes -->
  <xsl:template match="NOTE">
    <aside class="note">
      <xsl:apply-templates/>
    </aside>
  </xsl:template>

  <!-- Catch-all template for text nodes -->
  <xsl:template match="text()">
    <xsl:value-of select="normalize-space(.)"/>
  </xsl:template>

</xsl:stylesheet>
