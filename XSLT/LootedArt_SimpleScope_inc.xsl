<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

	<xsl:output method="html"/>

	<xsl:template match="version">
		<!--
		VERSION CONTROL	SeamenMedal_SimpleScope_XSL XSL STYLESHEET
	
		###	VERSION: 1.0 	AUTHOR: CDICKSON	DATE: 08/07/2004
		Created.
		-->
	</xsl:template>

	<!-- ignore 'doctype' text (should be 'LA') -->
	<xsl:template mode="LootedArt" match="emph[@altrender='doctype']">
	</xsl:template>


	<!-- add Scope -->
	<xsl:template mode="LootedArt" match="emph[@altrender='scope']">
	 <xsl:variable name="scope" select="."/>
	 <tr class="medalRow">
	  <td class="medalplain" width="0%"></td>
	  <td class="medalplain" width="100%">
	   <!-- Truncate at the first available space between characters 110 and 120, otherwise at character 120 regardless -->
	   <xsl:choose>
 	    <xsl:when test="substring($scope,110,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($scope,1,110)" />
	     <xsl:if test="string-length($scope) &gt; 110">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($scope,111,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($scope,1,111)" />
	     <xsl:if test="string-length($scope) &gt; 111">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($scope,112,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($scope,1,112)" />
	     <xsl:if test="string-length($scope) &gt; 112">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($scope,113,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($scope,1,113)" />
	     <xsl:if test="string-length($scope) &gt; 113">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($scope,114,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($scope,1,114)" />
	     <xsl:if test="string-length($scope) &gt; 114">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($scope,115,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($scope,1,115)" />
	     <xsl:if test="string-length($scope) &gt; 115">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($scope,116,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($scope,1,116)" />
	     <xsl:if test="string-length($scope) &gt; 116">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($scope,117,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($scope,1,117)" />
	     <xsl:if test="string-length($scope) &gt; 117">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($scope,118,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($scope,1,118)" />
	     <xsl:if test="string-length($scope) &gt; 118">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($scope,119,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($scope,1,119)" />
	     <xsl:if test="string-length($scope) &gt; 119">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:otherwise>
 	     <xsl:value-of disable-output-escaping="yes" select="substring($scope,1,120)" />
	     <xsl:if test="string-length($scope) &gt; 120">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:otherwise>
	   </xsl:choose>
	  </td>
	 </tr>
	</xsl:template>		
	
</xsl:stylesheet>

