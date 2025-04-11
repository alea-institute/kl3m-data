<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0"
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

  <!-- Output valid, indented HTML with UTF-8 encoding -->
  <xsl:output method="html" encoding="UTF-8" indent="yes"/>
  <xsl:strip-space elements="*"/>

  <!-- Root template: create full HTML page with head and body -->
  <xsl:template match="/">
    <html>
      <head>
        <meta charset="UTF-8"/>
        <title>
          <!-- Use the first TITLE/P for the head title (text-only mode) -->
          <xsl:apply-templates select="(//TITLE//P)[1]" mode="text"/>
        </title>
      </head>
      <body>
        <!-- Process TITLE section if available -->
        <xsl:if test="//TITLE">
          <div id="title">
            <xsl:apply-templates select="(//TITLE)[1]"/>
          </div>
        </xsl:if>
        <!-- Process CONTENTS section if available -->
        <xsl:if test="//CONTENTS">
          <div id="contents">
            <xsl:apply-templates select="(//CONTENTS)[1]"/>
          </div>
        </xsl:if>
        <!-- Otherwise, process the whole document -->
        <xsl:if test="not(//TITLE) and not(//CONTENTS)">
          <div id="content">
            <xsl:apply-templates/>
          </div>
        </xsl:if>
      </body>
    </html>
  </xsl:template>

  <!-- TITLE section: wrap in a container -->
  <xsl:template match="TITLE">
    <div class="title">
      <xsl:apply-templates/>
    </div>
  </xsl:template>

  <!-- Process the TI element: use the first P as an H1 (inline) and subsequent P as normal paragraphs -->
  <xsl:template match="TI">
    <xsl:choose>
      <xsl:when test="P">
        <h1>
          <xsl:apply-templates select="P[1]" mode="inline"/>
        </h1>
        <xsl:apply-templates select="P[position()>1]"/>
      </xsl:when>
      <xsl:otherwise>
        <xsl:apply-templates/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <!-- Mode for extracting text-only for the head title -->
  <xsl:template match="P" mode="text">
    <xsl:value-of select="normalize-space(.)"/>
  </xsl:template>

  <!-- Standard paragraph processing -->
  <xsl:template match="P">
    <p>
      <xsl:apply-templates/>
    </p>
  </xsl:template>

  <!-- Inline mode for P: output content without wrapping in <p> -->
  <xsl:template match="P" mode="inline">
    <xsl:apply-templates mode="inline"/>
  </xsl:template>

  <!-- NP element: render enumeration in a paragraph -->
  <xsl:template match="NP">
    <p>
      <strong>
        <xsl:apply-templates select="NO.P"/>
      </strong>
      <xsl:text>&#160;</xsl:text>
      <xsl:apply-templates select="TXT"/>
    </p>
  </xsl:template>

  <!-- Inline mode for NP: output enumeration without wrapping -->
  <xsl:template match="NP" mode="inline">
    <strong>
      <xsl:apply-templates select="NO.P" mode="inline"/>
    </strong>
    <xsl:text>&#160;</xsl:text>
    <xsl:apply-templates select="TXT" mode="inline"/>
  </xsl:template>

  <!-- TXT element: output its text content -->
  <xsl:template match="TXT">
    <xsl:value-of select="."/>
  </xsl:template>

  <!-- Inline mode for TXT -->
  <xsl:template match="TXT" mode="inline">
    <xsl:value-of select="."/>
  </xsl:template>

  <!-- Convert description lists to HTML unordered lists -->
  <xsl:template match="DLIST">
    <ul>
      <xsl:apply-templates select="DLIST.ITEM"/>
    </ul>
  </xsl:template>

  <xsl:template match="DLIST.ITEM">
    <li>
      <strong>
        <xsl:apply-templates select="TERM"/>
      </strong>
      <xsl:text>:&#160;</xsl:text>
      <xsl:apply-templates select="DEFINITION"/>
    </li>
  </xsl:template>

  <!-- Convert TBL elements to tables -->
  <xsl:template match="TBL">
    <table border="1">
      <xsl:apply-templates select="CORPUS/ROW"/>
    </table>
  </xsl:template>

  <xsl:template match="ROW">
    <tr>
      <xsl:apply-templates select="CELL"/>
    </tr>
  </xsl:template>

  <xsl:template match="CELL">
    <td>
      <xsl:apply-templates/>
    </td>
  </xsl:template>

  <!-- GR.SEQ elements: group container -->
  <xsl:template match="GR.SEQ">
    <div class="group">
      <xsl:apply-templates/>
    </div>
  </xsl:template>

  <!-- Titles within GR.SEQ rendered as sub-headings using inline processing -->
  <xsl:template match="GR.SEQ/TITLE">
    <h2>
      <xsl:apply-templates select="node()" mode="inline"/>
    </h2>
  </xsl:template>

  <!-- HT elements: use em for italics and strong for uppercase -->
  <xsl:template match="HT[@TYPE='ITALIC']">
    <em>
      <xsl:apply-templates/>
    </em>
  </xsl:template>

  <xsl:template match="HT[@TYPE='UC']">
    <strong>
      <xsl:apply-templates/>
    </strong>
  </xsl:template>

  <xsl:template match="HT">
    <strong>
      <xsl:apply-templates/>
    </strong>
  </xsl:template>

  <!-- Inline mode for HT -->
  <xsl:template match="HT" mode="inline">
    <strong>
      <xsl:apply-templates mode="inline"/>
    </strong>
  </xsl:template>

  <!-- Render NOTE elements in emphasis and wrap in parentheses -->
  <xsl:template match="NOTE">
    <em>
      <xsl:text>(</xsl:text>
      <xsl:apply-templates/>
      <xsl:text>)</xsl:text>
    </em>
  </xsl:template>

  <!-- Inline mode for NOTE -->
  <xsl:template match="NOTE" mode="inline">
    <em>
      <xsl:text>(</xsl:text>
      <xsl:apply-templates mode="inline"/>
      <xsl:text>)</xsl:text>
    </em>
  </xsl:template>

  <!-- Render addresses using a div -->
  <xsl:template match="ADDR.S">
    <div class="address">
      <xsl:apply-templates/>
    </div>
  </xsl:template>

  <!-- DATE and FT: output their content -->
  <xsl:template match="DATE | FT">
    <xsl:apply-templates/>
  </xsl:template>

  <!-- Render NO.DOC.C as a paragraph -->
  <xsl:template match="NO.DOC.C">
    <p>
      <xsl:apply-templates/>
    </p>
  </xsl:template>

  <!-- Ignore QUOT.START and QUOT.END tags, output their content -->
  <xsl:template match="QUOT.START | QUOT.END">
    <xsl:apply-templates/>
  </xsl:template>

  <!-- Default catch-all: process children -->
  <xsl:template match="*">
    <xsl:apply-templates/>
  </xsl:template>

  <!-- Default inline mode catch-all: process children without wrapping -->
  <xsl:template match="*" mode="inline">
    <xsl:apply-templates mode="inline"/>
  </xsl:template>

  <!-- Process text nodes -->
  <xsl:template match="text()">
    <xsl:value-of select="."/>
  </xsl:template>

  <!-- Inline mode for text nodes -->
  <xsl:template match="text()" mode="inline">
    <xsl:value-of select="."/>
  </xsl:template>

</xsl:stylesheet>
