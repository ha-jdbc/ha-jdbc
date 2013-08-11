/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc.dialect;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Set;
import java.util.regex.Pattern;

import net.sf.hajdbc.IdentifierNormalizer;

public class StandardIdentifierNormalizer implements IdentifierNormalizer
{
	private static final Pattern UPPER_CASE_PATTERN = Pattern.compile("[A-Z]");
	private static final Pattern LOWER_CASE_PATTERN = Pattern.compile("[a-z]");
	
	private final Pattern identifierPattern;
	private final Set<String> reservedIdentifiers;
	private final String quote;
	private final boolean supportsMixedCaseIdentifiers;
	private final boolean supportsMixedCaseQuotedIdentifiers;
	private final boolean storesLowerCaseIdentifiers;
	private final boolean storesLowerCaseQuotedIdentifiers;
	private final boolean storesUpperCaseIdentifiers;
	private final boolean storesUpperCaseQuotedIdentifiers;
	
	public StandardIdentifierNormalizer(DatabaseMetaData metaData, Pattern identifierPattern, Set<String> reservedIdentifiers) throws SQLException
	{
		this.identifierPattern = identifierPattern;
		this.reservedIdentifiers = reservedIdentifiers;
		this.quote = metaData.getIdentifierQuoteString();
		this.supportsMixedCaseIdentifiers = metaData.supportsMixedCaseIdentifiers();
		this.supportsMixedCaseQuotedIdentifiers = metaData.supportsMixedCaseQuotedIdentifiers();
		this.storesLowerCaseIdentifiers = metaData.storesLowerCaseIdentifiers();
		this.storesLowerCaseQuotedIdentifiers = metaData.storesLowerCaseQuotedIdentifiers();
		this.storesUpperCaseIdentifiers = metaData.storesUpperCaseIdentifiers();
		this.storesUpperCaseQuotedIdentifiers = metaData.storesUpperCaseQuotedIdentifiers();
	}

	@Override
	public String normalize(String identifier)
	{
		if (identifier == null) return null;
		
		int quoteLength = this.quote.length();
		
		boolean quoted = identifier.startsWith(this.quote) && identifier.endsWith(this.quote);
		// Strip any existing quoting
		String raw = quoted ? identifier.substring(quoteLength, identifier.length() - quoteLength) : identifier;
		
		// Quote reserved identifiers
		boolean requiresQuoting = this.reservedIdentifiers.contains(raw.toUpperCase());
		
		// Quote identifiers containing special characters
		requiresQuoting |= !this.identifierPattern.matcher(raw).matches();
		
		// Quote mixed-case identifiers if detected and supported by DBMS
		requiresQuoting |= quoted && !this.supportsMixedCaseIdentifiers && this.supportsMixedCaseQuotedIdentifiers && ((this.storesLowerCaseIdentifiers && !this.storesLowerCaseQuotedIdentifiers && UPPER_CASE_PATTERN.matcher(raw).find()) || (this.storesUpperCaseIdentifiers && !this.storesUpperCaseQuotedIdentifiers && LOWER_CASE_PATTERN.matcher(raw).find()));
		
		return requiresQuoting ? this.quote + this.normalizeCaseQuoted(raw) + this.quote : this.normalizeCase(raw);
	}

	private String normalizeCase(String identifier)
	{
		if (this.storesLowerCaseIdentifiers) return identifier.toLowerCase();
		
		if (this.storesUpperCaseIdentifiers) return identifier.toUpperCase();
		
		return identifier;
	}

	private String normalizeCaseQuoted(String identifier)
	{
		if (this.storesLowerCaseQuotedIdentifiers) return identifier.toLowerCase();
		
		if (this.storesUpperCaseQuotedIdentifiers) return identifier.toUpperCase();
		
		return identifier;
	}
}
