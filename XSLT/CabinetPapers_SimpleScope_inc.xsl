<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

	<xsl:output method="html"/>

	<xsl:template match="version">
		<!--
		VERSION CONTROL	CabinetPapers_SimpleScope_XSL XSL STYLESHEET
	
		###	VERSION: 1.0 	AUTHOR: CDICKSON	DATE: 08/07/2004
		Created.
		###	VERSION: 1.1 	AUTHOR: MHILLYARD	DATE: 23/03/2006
		Modified.
		###	VERSION: 1.2 	AUTHOR: MHILLYARD	DATE: 25/04/2007
		Modified.
		-->
	</xsl:template>


	<!-- ignore 'doctype' text (should be 'CP') -->
	<xsl:template mode="CabinetPapers" match="emph[@altrender='doctype']">
	</xsl:template>


	<!-- add surname mentioned: -->
	<xsl:template mode="CabinetPapers" match="emph[@altrender='surname']">
	 <xsl:variable name="surname"> 
	  <xsl:for-each select="./surname">
	   <xsl:value-of disable-output-escaping="yes" select="text()" />
	   <xsl:if test="following-sibling::surname">
	    <xsl:text disable-output-escaping="yes">; </xsl:text>
	   </xsl:if>
	  </xsl:for-each>
	 </xsl:variable>

	 <tr class="medalRow">
	  <td class="medalplain" width="20%"> Attendees: </td>
	  <td class="medalplain" width="50%">
	   <!-- Truncate at the first available space between characters 120 and 130, otherwise at character 130 regardless -->
	   <xsl:choose>
 	    <xsl:when test="substring($surname,120,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($surname,1,120)" />
	     <xsl:if test="string-length($surname) &gt; 120">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($surname,121,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($surname,1,121)" />
	     <xsl:if test="string-length($surname) &gt; 121">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($surname,122,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($surname,1,122)" />
	     <xsl:if test="string-length($surname) &gt; 122">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($surname,123,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($surname,1,123)" />
	     <xsl:if test="string-length($surname) &gt; 123">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($surname,124,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($surname,1,124)" />
	     <xsl:if test="string-length($surname) &gt; 124">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($surname,125,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($surname,1,125)" />
	     <xsl:if test="string-length($surname) &gt; 125">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($surname,126,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($surname,1,126)" />
	     <xsl:if test="string-length($surname) &gt; 126">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($surname,127,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($surname,1,127)" />
	     <xsl:if test="string-length($surname) &gt; 127">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($surname,128,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($surname,1,128)" />
	     <xsl:if test="string-length($surname) &gt; 128">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($surname,129,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($surname,1,129)" />
	     <xsl:if test="string-length($surname) &gt; 129">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:otherwise>
 	     <xsl:value-of disable-output-escaping="yes" select="substring($surname,1,130)" />
	     <xsl:if test="string-length($surname) &gt; 130">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:otherwise>
	   </xsl:choose>
	  </td>
	 </tr>
	</xsl:template>		


	<!-- add Agenda: -->
	<xsl:template mode="CabinetPapers" match="emph[@altrender='agenda']">
	 <xsl:variable name="agenda" select="."/>
	 <tr class="medalRow">
	  <td class="medalplain" width="20%"> Agenda: </td>
	  <td class="medalplain" width="50%">
	   <!-- Truncate at the first available space between characters 110 and 120, otherwise at character 120 regardless -->
	   <xsl:choose>
 	    <xsl:when test="substring($agenda,110,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($agenda,1,110)" />
	     <xsl:if test="string-length($agenda) &gt; 110">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($agenda,111,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($agenda,1,111)" />
	     <xsl:if test="string-length($agenda) &gt; 111">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($agenda,112,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($agenda,1,112)" />
	     <xsl:if test="string-length($agenda) &gt; 112">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($agenda,113,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($agenda,1,113)" />
	     <xsl:if test="string-length($agenda) &gt; 113">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($agenda,114,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($agenda,1,114)" />
	     <xsl:if test="string-length($agenda) &gt; 114">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($agenda,115,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($agenda,1,115)" />
	     <xsl:if test="string-length($agenda) &gt; 115">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($agenda,116,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($agenda,1,116)" />
	     <xsl:if test="string-length($agenda) &gt; 116">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($agenda,117,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($agenda,1,117)" />
	     <xsl:if test="string-length($agenda) &gt; 117">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($agenda,118,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($agenda,1,118)" />
	     <xsl:if test="string-length($agenda) &gt; 118">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($agenda,119,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($agenda,1,119)" />
	     <xsl:if test="string-length($agenda) &gt; 119">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:otherwise>
 	     <xsl:value-of disable-output-escaping="yes" select="substring($agenda,1,120)" />
	     <xsl:if test="string-length($agenda) &gt; 120">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:otherwise>
	   </xsl:choose>
	  </td>
	 </tr>
	</xsl:template>		

	<!-- add Former Reference  -->
	<xsl:template mode="CabinetPapers" match="emph[@altrender='formerreference']">
			<tr class="medalRow">
			<td class="medalplain">  Former Reference: </td>
			<td class="medalplain"><xsl:value-of select="text()" />
		</td></tr>
	</xsl:template>	

	<!-- add Record Type  -->
	<xsl:template mode="CabinetPapers" match="emph[@altrender='type']">
			<tr class="medalRow">
			<td class="medalplain">  Record Type: </td>
			<td class="medalplain"><xsl:value-of select="text()" />
		</td></tr>
	</xsl:template>	
	

	<!-- add title: -->
	<xsl:template mode="CabinetPapers" match="emph[@altrender='title']">
	 <xsl:variable name="title" select="."/>
	 <tr class="medalRow">
	  <td class="medalplain" width="20%"> Title: </td>
	  <td class="medalplain" width="50%">
	   <!-- Truncate at the first available space between characters 110 and 120, otherwise at character 120 regardless -->
	   <xsl:choose>
 	    <xsl:when test="substring($title,110,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($title,1,110)" />
	     <xsl:if test="string-length($title) &gt; 110">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($title,111,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($title,1,111)" />
	     <xsl:if test="string-length($title) &gt; 111">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($title,112,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($title,1,112)" />
	     <xsl:if test="string-length($title) &gt; 112">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($title,113,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($title,1,113)" />
	     <xsl:if test="string-length($title) &gt; 113">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($title,114,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($title,1,114)" />
	     <xsl:if test="string-length($title) &gt; 114">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($title,115,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($title,1,115)" />
	     <xsl:if test="string-length($title) &gt; 115">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($title,116,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($title,1,116)" />
	     <xsl:if test="string-length($title) &gt; 116">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($title,117,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($title,1,117)" />
	     <xsl:if test="string-length($title) &gt; 117">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($title,118,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($title,1,118)" />
	     <xsl:if test="string-length($title) &gt; 118">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:when test="substring($title,119,1) = ' '">
 	     <xsl:value-of disable-output-escaping="yes" select="substring($title,1,119)" />
	     <xsl:if test="string-length($title) &gt; 119">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:when>
 	    <xsl:otherwise>
 	     <xsl:value-of disable-output-escaping="yes" select="substring($title,1,120)" />
	     <xsl:if test="string-length($title) &gt; 120">
	      <xsl:text>...</xsl:text>
	     </xsl:if>
	    </xsl:otherwise>
	   </xsl:choose>
	  </td>
	 </tr>
	</xsl:template>	

	<!-- add Author  -->
	<xsl:template mode="CabinetPapers" match="emph[@altrender='author']">
			<tr class="medalRow">
			<td class="medalplain">  Author: </td>
			<td class="medalplain"><xsl:value-of select="text()" />
		</td></tr>
	</xsl:template>		
</xsl:stylesheet>
