<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0"
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns="http://www.w3.org/1999/xhtml">

<xsl:output method="html" encoding="UTF-8" indent="yes" doctype-system="about:legacy-compat"/>

<xsl:template match="/">
    <html lang="en">
        <head>
            <meta charset="UTF-8"/>
            <title>
                <xsl:value-of select="//TITLE/TI/P[1]"/>
            </title>
        </head>
        <body>
            <xsl:apply-templates select="//GENERAL | //DOC | //PUBLICATION"/>
        </body>
    </html>
</xsl:template>

<xsl:template match="GENERAL">
    <article>
        <xsl:apply-templates select="TITLE"/>
        <xsl:apply-templates select="CONTENTS"/>
    </article>
</xsl:template>

<xsl:template match="DOC">
    <article>
        <xsl:apply-templates select="PUBLICATION.REF"/>
        <xsl:apply-templates select="//TITLE"/>
    </article>
</xsl:template>

<xsl:template match="PUBLICATION">
    <article>
        <xsl:apply-templates select="//SECTION"/>
    </article>
</xsl:template>

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

<xsl:template match="TITLE">
    <h1><xsl:apply-templates select="TI/P"/></h1>
</xsl:template>

<xsl:template match="CONTENTS">
    <section>
        <xsl:apply-templates/>
    </section>
</xsl:template>

<xsl:template match="SECTION">
    <section>
        <xsl:apply-templates select="TITLE"/>
        <xsl:apply-templates select="SUBSECTION"/>
    </section>
</xsl:template>

<xsl:template match="SUBSECTION">
    <section>
        <xsl:apply-templates select="TITLE"/>
        <xsl:apply-templates select="ITEM.PUB"/>
    </section>
</xsl:template>

<xsl:template match="P">
    <p><xsl:apply-templates/></p>
</xsl:template>

<xsl:template match="LIST">
    <ul>
        <xsl:apply-templates/>
    </ul>
</xsl:template>

<xsl:template match="ITEM">
    <li><xsl:apply-templates/></li>
</xsl:template>

<xsl:template match="DATE">
    <time datetime="{@ISO}"><xsl:value-of select="."/></time>
</xsl:template>

<xsl:template match="HT[@TYPE='ITALIC']">
    <em><xsl:apply-templates/></em>
</xsl:template>

<!-- Catch-all template for text nodes -->
<xsl:template match="text()">
    <xsl:value-of select="normalize-space(.)"/>
</xsl:template>

</xsl:stylesheet>
