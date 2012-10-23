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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import net.sf.hajdbc.IdentifierNormalizer;
import net.sf.hajdbc.util.Strings;

public class StandardIdentifierNormalizer implements IdentifierNormalizer
{
	// As defined in SQL-92 specification: http://www.andrew.cmu.edu/user/shadow/sql/sql1992.txt
	private static final String[] SQL_92_RESERVED_WORDS = new String[] {
		"ABSOLUTE", "ACTION", "ADD", "ALL", "ALLOCATE", "ALTER", "AND", "ANY", "ARE", "AS", "ASC", "ASSERTION", "AT", "AUTHORIZATION", "AVG",
		"BEGIN", "BETWEEN", "BIT", "BIT_LENGTH", "BOTH", "BY",
		"CASCADE", "CASCADED", "CASE", "CAST", "CATALOG", "CHAR", "CHARACTER", "CHAR_LENGTH", "CHARACTER_LENGTH", "CHECK", "CLOSE", "COALESCE", "COLLATE", "COLLATION", "COLUMN", "COMMIT", "CONNECT", "CONNECTION", "CONSTRAINT", "CONSTRAINTS", "CONTINUE", "CONVERT", "CORRESPONDING", "COUNT", "CREATE", "CROSS", "CURRENT", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR",
		"DATE", "DAY", "DEALLOCATE", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DEFERRABLE", "DEFERRED", "DELETE", "DESC", "DESCRIBE", "DESCRIPTOR", "DIAGNOSTICS", "DISCONNECT", "DISTINCT", "DOMAIN", "DOUBLE", "DROP",
		"ELSE", "END", "END-EXEC", "ESCAPE", "EXCEPT", "EXCEPTION", "EXEC", "EXECUTE", "EXISTS", "EXTERNAL", "EXTRACT",
		"FALSE", "FETCH", "FIRST", "FLOAT", "FOR", "FOREIGN", "FOUND", "FROM", "FULL",
		"GET", "GLOBAL", "GO", "GOTO", "GRANT", "GROUP",
		"HAVING", "HOUR",
		"IDENTITY", "IMMEDIATE", "IN", "INDICATOR", "INITIALLY", "INNER", "INPUT", "INSENSITIVE", "INSERT", "INT", "INTEGER", "INTERSECT", "INTERVAL", "INTO", "IS", "ISOLATION",
		"JOIN",
		"KEY",
		"LANGUAGE", "LAST", "LEADING", "LEFT", "LEVEL", "LIKE", "LOCAL", "LOWER",
		"MATCH", "MAX", "MIN", "MINUTE", "MODULE", "MONTH",
		"NAMES", "NATIONAL", "NATURAL", "NCHAR", "NEXT", "NO", "NOT", "NULL", "NULLIF", "NUMERIC",
		"OCTET_LENGTH", "OF", "ON", "ONLY", "OPEN", "OPTION", "OR", "ORDER", "OUTER", "OUTPUT", "OVERLAPS",
		"PAD", "PARTIAL", "POSITION", "PRECISION", "PREPARE", "PRESERVE", "PRIMARY", "PRIOR", "PRIVILEGES", "PROCEDURE", "PUBLIC",
		"READ", "REAL", "REFERENCES", "RELATIVE", "RESTRICT", "REVOKE", "RIGHT", "ROLLBACK", "ROWS",
		"SCHEMA", "SCROLL", "SECOND", "SECTION", "SELECT", "SESSION", "SESSION_USER", "SET", "SIZE", "SMALLINT", "SOME", "SPACE", "SQL", "SQLCODE", "SQLERROR", "SQLSTATE", "SUBSTRING", "SUM", "SYSTEM_USER",
		"TABLE", "TEMPORARY", "THEN", "TIME", "TIMESTAMP", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TO", "TRAILING", "TRANSACTION", "TRANSLATE", "TRANSLATION", "TRIM", "TRUE",
		"UNION", "UNIQUE", "UNKNOWN", "UPDATE", "UPPER", "USAGE", "USER", "USING",
		"VALUE", "VALUES", "VARCHAR", "VARYING", "VIEW",
		"WHEN", "WHENEVER", "WHERE", "WITH", "WORK", "WRITE",
		"YEAR",
		"ZONE"
	};
	
	private static final Pattern UPPER_CASE_PATTERN = Pattern.compile("[A-Z]");
	private static final Pattern LOWER_CASE_PATTERN = Pattern.compile("[a-z]");
	
	private final Set<String> reservedIdentifierSet = new HashSet<String>(SQL_92_RESERVED_WORDS.length);
	private final Pattern identifierPattern;
	private final String quote;
	private final boolean supportsMixedCaseIdentifiers;
	private final boolean supportsMixedCaseQuotedIdentifiers;
	private final boolean storesLowerCaseIdentifiers;
	private final boolean storesLowerCaseQuotedIdentifiers;
	private final boolean storesUpperCaseIdentifiers;
	private final boolean storesUpperCaseQuotedIdentifiers;
	
	public StandardIdentifierNormalizer(DatabaseMetaData metaData, Pattern identifierPattern) throws SQLException
	{
		this.identifierPattern = identifierPattern;
		this.quote = metaData.getIdentifierQuoteString();
		this.supportsMixedCaseIdentifiers = metaData.supportsMixedCaseIdentifiers();
		this.supportsMixedCaseQuotedIdentifiers = metaData.supportsMixedCaseQuotedIdentifiers();
		this.storesLowerCaseIdentifiers = metaData.storesLowerCaseIdentifiers();
		this.storesLowerCaseQuotedIdentifiers = metaData.storesLowerCaseQuotedIdentifiers();
		this.storesUpperCaseIdentifiers = metaData.storesUpperCaseIdentifiers();
		this.storesUpperCaseQuotedIdentifiers = metaData.storesUpperCaseQuotedIdentifiers();
		
		this.reservedIdentifierSet.addAll(Arrays.asList(SQL_92_RESERVED_WORDS));
		
		for (String word: metaData.getSQLKeywords().split(Strings.COMMA))
		{
			this.reservedIdentifierSet.add(word.toUpperCase());
		}
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
		boolean requiresQuoting = this.reservedIdentifierSet.contains(raw.toUpperCase());
		
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
