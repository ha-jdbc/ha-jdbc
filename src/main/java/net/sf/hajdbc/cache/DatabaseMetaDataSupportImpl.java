/*
 * HA-JDBC: High-Availability JDBC
 * Copyright 2004-2009 Paul Ferraro
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
package net.sf.hajdbc.cache;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.util.Strings;

/**
 * Processes database meta data into useful structures.
 * @author Paul Ferraro
 */
@SuppressWarnings("nls")
public class DatabaseMetaDataSupportImpl implements DatabaseMetaDataSupport
{
	private static final long serialVersionUID = 7296994314643391105L;

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
	
	private final Dialect dialect;
	private final Set<String> reservedIdentifierSet = new HashSet<String>(SQL_92_RESERVED_WORDS.length);
	private final Pattern identifierPattern;
	private final String quote;
	private final boolean supportsMixedCaseIdentifiers;
	private final boolean supportsMixedCaseQuotedIdentifiers;
	private final boolean storesLowerCaseIdentifiers;
	private final boolean storesLowerCaseQuotedIdentifiers;
	private final boolean storesUpperCaseIdentifiers;
	private final boolean storesUpperCaseQuotedIdentifiers;
	private final boolean supportsSchemasInDDL;
	private final boolean supportsSchemasInDML;
	
	/**
	 * Constructs a new DatabaseMetaDataSupport using the specified DatabaseMetaData implementation.
	 * @param metaData a DatabaseMetaData implementation
	 * @param dialect the vendor-specific dialect of the cluster
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	public DatabaseMetaDataSupportImpl(DatabaseMetaData metaData, Dialect dialect) throws SQLException
	{
		this.dialect = dialect;
		
		this.identifierPattern = dialect.getIdentifierPattern(metaData);
		this.quote = metaData.getIdentifierQuoteString();
		this.supportsMixedCaseIdentifiers = metaData.supportsMixedCaseIdentifiers();
		this.supportsMixedCaseQuotedIdentifiers = metaData.supportsMixedCaseQuotedIdentifiers();
		this.storesLowerCaseIdentifiers = metaData.storesLowerCaseIdentifiers();
		this.storesLowerCaseQuotedIdentifiers = metaData.storesLowerCaseQuotedIdentifiers();
		this.storesUpperCaseIdentifiers = metaData.storesUpperCaseIdentifiers();
		this.storesUpperCaseQuotedIdentifiers = metaData.storesUpperCaseQuotedIdentifiers();
		this.supportsSchemasInDML = metaData.supportsSchemasInDataManipulation();
		this.supportsSchemasInDDL = metaData.supportsSchemasInTableDefinitions();
		
		this.reservedIdentifierSet.addAll(Arrays.asList(SQL_92_RESERVED_WORDS));
		
		for (String word: metaData.getSQLKeywords().split(Strings.COMMA))
		{
			this.reservedIdentifierSet.add(word.toUpperCase());
		}
	}
	
	/**
	 * Returns all tables in this database mapped by schema.
	 * @param metaData a DatabaseMetaData implementation
	 * @return a Map of schema name to Collection of table names
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	@Override
	public Collection<QualifiedName> getTables(DatabaseMetaData metaData) throws SQLException
	{
		List<QualifiedName> list = new LinkedList<QualifiedName>();
		
		ResultSet resultSet = metaData.getTables(this.getCatalog(metaData), null, Strings.ANY, new String[] { "TABLE" });
		
		while (resultSet.next())
		{
			list.add(new QualifiedName(resultSet.getString("TABLE_SCHEM"), resultSet.getString("TABLE_NAME")));
		}
		
		resultSet.close();
		
		return list;
	}

	/**
	 * Returns the columns of the specified table.
	 * @param metaData a DatabaseMetaData implementation
	 * @param table a schema qualified table name
	 * @return a Map of column name to column properties
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	@Override
	public Map<String, ColumnProperties> getColumns(DatabaseMetaData metaData, QualifiedName table) throws SQLException
	{
		Map<String, ColumnProperties> columnMap = new HashMap<String, ColumnProperties>();
		
		Statement statement = metaData.getConnection().createStatement();
		
		try
		{
			ResultSetMetaData resultSet = statement.executeQuery(String.format("SELECT * FROM %s WHERE 0=1", this.qualifyNameForDML(table))).getMetaData();
			
			for (int i = 1; i <= resultSet.getColumnCount(); ++i)
			{
				String column = this.quote(resultSet.getColumnName(i));
				int type = resultSet.getColumnType(i);
				String nativeType = resultSet.getColumnTypeName(i);
				boolean autoIncrement = resultSet.isAutoIncrement(i);
				
				columnMap.put(column, new ColumnPropertiesImpl(column, type, nativeType, null, null, autoIncrement));
			}
		}
		finally
		{
			statement.close();
		}
		
		return columnMap;
	}

	/**
	 * Returns the primary key of the specified table.
	 * @param metaData a DatabaseMetaData implementation
	 * @param table a schema qualified table name
	 * @return a unique constraint
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	@Override
	public UniqueConstraint getPrimaryKey(DatabaseMetaData metaData, QualifiedName table) throws SQLException
	{
		UniqueConstraint constraint = null;

		ResultSet resultSet = metaData.getPrimaryKeys(this.getCatalog(metaData), this.getSchema(table), table.getName());
		
		while (resultSet.next())
		{
			String name = this.quote(resultSet.getString("PK_NAME"));

			if (constraint == null)
			{
				constraint = new UniqueConstraintImpl(name, this.qualifyNameForDDL(table));
			}
			
			String column = this.quote(resultSet.getString("COLUMN_NAME"));
			
			constraint.getColumnList().add(column);
		}
		
		resultSet.close();
		
		return constraint;
	}

	/**
	 * Returns the foreign key constraints on the specified table.
	 * @param metaData a DatabaseMetaData implementation
	 * @param table a schema qualified table name
	 * @return a Collection of foreign key constraints.
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	@Override
	public Collection<ForeignKeyConstraint> getForeignKeyConstraints(DatabaseMetaData metaData, QualifiedName table) throws SQLException
	{
		Map<String, ForeignKeyConstraint> foreignKeyMap = new HashMap<String, ForeignKeyConstraint>();
		
		ResultSet resultSet = metaData.getImportedKeys(this.getCatalog(metaData), this.getSchema(table), table.getName());
		
		while (resultSet.next())
		{
			String name = this.quote(resultSet.getString("FK_NAME"));
			
			ForeignKeyConstraint foreignKey = foreignKeyMap.get(name);
			
			if (foreignKey == null)
			{
				foreignKey = new ForeignKeyConstraintImpl(name, this.qualifyNameForDDL(table));
				
				String foreignSchema = this.quote(resultSet.getString("PKTABLE_SCHEM"));
				String foreignTable = this.quote(resultSet.getString("PKTABLE_NAME"));
				
				foreignKey.setForeignTable(this.qualifyNameForDDL(new QualifiedName(foreignSchema, foreignTable)));
				foreignKey.setDeleteRule(resultSet.getInt("DELETE_RULE"));
				foreignKey.setUpdateRule(resultSet.getInt("UPDATE_RULE"));
				foreignKey.setDeferrability(resultSet.getInt("DEFERRABILITY"));
				
				foreignKeyMap.put(name, foreignKey);
			}
			
			String column = this.quote(resultSet.getString("FKCOLUMN_NAME"));
			String foreignColumn = this.quote(resultSet.getString("PKCOLUMN_NAME"));

			foreignKey.getColumnList().add(column);
			foreignKey.getForeignColumnList().add(foreignColumn);
		}
		
		resultSet.close();
		
		return foreignKeyMap.values();
	}

	/**
	 * Returns the unique constraints on the specified table - excluding the primary key of the table.
	 * @param metaData a schema qualified table name
	 * @param table a qualified table name
	 * @param primaryKey the primary key of this table
	 * @return a Collection of unique constraints.
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	@Override
	public Collection<UniqueConstraint> getUniqueConstraints(DatabaseMetaData metaData, QualifiedName table, UniqueConstraint primaryKey) throws SQLException
	{
		Map<String, UniqueConstraint> keyMap = new HashMap<String, UniqueConstraint>();
		
		ResultSet resultSet = metaData.getIndexInfo(this.getCatalog(metaData), this.getSchema(table), table.getName(), true, false);
		
		while (resultSet.next())
		{
			if (resultSet.getShort("TYPE") == DatabaseMetaData.tableIndexHashed)
			{
				String name = this.quote(resultSet.getString("INDEX_NAME"));
				
				// Don't include the primary key
				if ((primaryKey != null) && name.equals(primaryKey.getName())) continue;
				
				UniqueConstraint key = keyMap.get(name);
				
				if (key == null)
				{
					key = new UniqueConstraintImpl(name, this.qualifyNameForDDL(table));
					
					keyMap.put(name, key);
				}
				
				String column = this.quote(resultSet.getString("COLUMN_NAME"));
				
				key.getColumnList().add(column);
			}
		}
		
		resultSet.close();
		
		return keyMap.values();
	}

	/**
	 * Returns the schema qualified name of the specified table suitable for use in a data modification language (DML) statement.
	 * @param name a schema qualified name
	 * @return a Collection of unique constraints.
	 */
	@Override
	public String qualifyNameForDML(QualifiedName name)
	{
		return this.qualifyName(name, this.supportsSchemasInDML);
	}

	/**
	 * Returns the schema qualified name of the specified table suitable for use in a data definition language (DDL) statement.
	 * @param name a schema qualified name
	 * @return a Collection of unique constraints.
	 */
	@Override
	public String qualifyNameForDDL(QualifiedName name)
	{
		return this.qualifyName(name, this.supportsSchemasInDDL);
	}

	private String qualifyName(QualifiedName name, boolean supportsSchemas)
	{
		StringBuilder builder = new StringBuilder();
		
		String schema = name.getSchema();
		
		if (supportsSchemas && (schema != null))
		{
			builder.append(this.quote(schema)).append(Strings.DOT);
		}
		
		return builder.append(this.quote(name.getName())).toString();
	}
	
	private String getCatalog(DatabaseMetaData metaData) throws SQLException
	{
		String catalog = metaData.getConnection().getCatalog();
		
		return (catalog != null) ? catalog : Strings.EMPTY;
	}
	
	private String getSchema(QualifiedName name)
	{
		String schema = name.getSchema();
		
		return (schema != null) ? schema : Strings.EMPTY;
	}
	
	private String quote(String identifier)
	{
		if (identifier == null) return null;
		
		int quoteLength = this.quote.length();
		
		// Strip any existing quoting
		String raw = (identifier.startsWith(this.quote) && identifier.endsWith(this.quote)) ? identifier.substring(quoteLength, identifier.length() - quoteLength) : identifier;
		
		// Quote reserved identifiers
		boolean requiresQuoting = this.reservedIdentifierSet.contains(raw.toUpperCase());
		
		// Quote identifiers containing special characters
		requiresQuoting |= !this.identifierPattern.matcher(raw).matches();
		
		// Quote mixed-case identifiers if detected and supported by DBMS
		requiresQuoting |= !this.supportsMixedCaseIdentifiers && this.supportsMixedCaseQuotedIdentifiers && ((this.storesLowerCaseIdentifiers && !this.storesLowerCaseQuotedIdentifiers && UPPER_CASE_PATTERN.matcher(raw).find()) || (this.storesUpperCaseIdentifiers && !this.storesUpperCaseQuotedIdentifiers && LOWER_CASE_PATTERN.matcher(raw).find()));
		
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
	
	private String normalize(String qualifiedName, String defaultSchema)
	{
		String parts[] = qualifiedName.split(Pattern.quote(Strings.DOT));

		String name = parts[parts.length - 1];
		String schema = (parts.length > 1) ? parts[parts.length - 2] : defaultSchema;
			
		return this.qualifyNameForDML(new QualifiedName(schema, name));
	}
	
	/**
	 * Returns a collection of sequences using dialect specific logic.
	 * @param metaData database meta data
	 * @return a collection of sequences
	 * @throws SQLException
	 */
	@Override
	public Collection<SequenceProperties> getSequences(DatabaseMetaData metaData) throws SQLException
	{
		Map<QualifiedName, Integer> sequences = this.dialect.getSequences(metaData);
		
		List<SequenceProperties> sequenceList = new ArrayList<SequenceProperties>(sequences.size());
		
		for (Map.Entry<QualifiedName, Integer> sequence: sequences.entrySet())
		{
			sequenceList.add(new SequencePropertiesImpl(this.qualifyNameForDML(sequence.getKey()), sequence.getValue()));
		}
		
		return sequenceList;
	}
	
	/**
	 * Locates an object from a map keyed by schema qualified name.
	 * @param <T> an object
	 * @param map a map of database 
	 * @param name the name of the object to locate
	 * @param defaultSchemaList a list of default schemas
	 * @return the object with the specified name
	 * @throws SQLException
	 */
	@Override
	public <T> T find(Map<String, T> map, String name, List<String> defaultSchemaList) throws SQLException
	{
		T properties = map.get(this.normalize(name, null));
		
		if (properties == null)
		{
			for (String schema: defaultSchemaList)
			{
				if (properties == null)
				{
					properties = map.get(this.normalize(name, schema));
				}
			}
		}
		
		if (properties == null)
		{
			throw new SQLException(Messages.SCHEMA_LOOKUP_FAILED.getMessage(name, defaultSchemaList, this.dialect.getClass().getName() + ".getDefaultSchemas()"));
		}
		
		return properties;
	}
	
	/**
	 * Identifies any identity columns from the from the specified collection of columns
	 * @param columns the columns of a table
	 * @return a collection of column names
	 * @throws SQLException
	 */
	@Override
	public Collection<String> getIdentityColumns(Collection<ColumnProperties> columns) throws SQLException
	{
		List<String> columnList = new LinkedList<String>();
		
		for (ColumnProperties column: columns)
		{
			if (column.isAutoIncrement())
			{
				columnList.add(column.getName());
			}
		}
		
		return columnList;
	}
}
