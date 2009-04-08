/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
 * 
 * This library is free software; you can redistribute it and/or modify it 
 * under the terms of the GNU Lesser General Public License as published by the 
 * Free Software Foundation; either version 2.1 of the License, or (at your 
 * option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License 
 * for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, 
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 * 
 * Contact: ferraro@users.sourceforge.net
 */
package net.sf.hajdbc.cache;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.SequenceProperties;
import net.sf.hajdbc.UniqueConstraint;
import net.sf.hajdbc.util.Strings;

/**
 * Processes database meta data into useful structures.
 * @author Paul Ferraro
 */
@SuppressWarnings("nls")
public class DatabaseMetaDataSupportImpl implements DatabaseMetaDataSupport
{
	// As defined in SQL-92 specification: http://www.andrew.cmu.edu/user/shadow/sql/sql1992.txt
	private static final String[] SQL_92_RESERVED_WORDS = new String[] {
		"absolute", "action", "add", "all", "allocate", "alter", "and", "any", "are", "as", "asc", "assertion", "at", "authorization", "avg",
		"begin", "between", "bit", "bit_length", "both", "by",
		"cascade", "cascaded", "case", "cast", "catalog", "char", "character", "char_length", "character_length", "check", "close", "coalesce", "collate", "collation", "column", "commit", "connect", "connection", "constraint", "constraints", "continue", "convert", "corresponding", "count", "create", "cross", "current", "current_date", "current_time", "current_timestamp", "current_user", "cursor",
		"date", "day", "deallocate", "dec", "decimal", "declare", "default", "deferrable", "deferred", "delete", "desc", "describe", "descriptor", "diagnostics", "disconnect", "distinct", "domain", "double", "drop",
		"else", "end", "end-exec", "escape", "except", "exception", "exec", "execute", "exists", "external", "extract",
		"false", "fetch", "first", "float", "for", "foreign", "found", "from", "full",
		"get", "global", "go", "goto", "grant", "group",
		"having", "hour",
		"identity", "immediate", "in", "indicator", "initially", "inner", "input", "insensitive", "insert", "int", "integer", "intersect", "interval", "into", "is", "isolation",
		"join",
		"key",
		"language", "last", "leading", "left", "level", "like", "local", "lower",
		"match", "max", "min", "minute", "module", "month",
		"names", "national", "natural", "nchar", "next", "no", "not", "null", "nullif", "numeric",
		"octet_length", "of", "on", "only", "open", "option", "or", "order", "outer", "output", "overlaps",
		"pad", "partial", "position", "precision", "prepare", "preserve", "primary", "prior", "privileges", "procedure", "public",
		"read", "real", "references", "relative", "restrict", "revoke", "right", "rollback", "rows",
		"schema", "scroll", "second", "section", "select", "session", "session_user", "set", "size", "smallint", "some", "space", "sql", "sqlcode", "sqlerror", "sqlstate", "substring", "sum", "system_user",
		"table", "temporary", "then", "time", "timestamp", "timezone_hour", "timezone_minute", "to", "trailing", "transaction", "translate", "translation", "trim", "true",
		"union", "unique", "unknown", "update", "upper", "usage", "user", "using",
		"value", "values", "varchar", "varying", "view",
		"when", "whenever", "where", "with", "work", "write",
		"year",
		"zone"
	};
	
	private static final Pattern UPPER_CASE_PATTERN = Pattern.compile("[A-Z]");
	private static final Pattern LOWER_CASE_PATTERN = Pattern.compile("[a-z]");
	
	private Dialect dialect;
	private Set<String> reservedIdentifierSet = new HashSet<String>();
	private Pattern identifierPattern;
	private String quote;
	private boolean supportsMixedCaseIdentifiers;
	private boolean supportsMixedCaseQuotedIdentifiers;
	private boolean storesLowerCaseIdentifiers;
	private boolean storesLowerCaseQuotedIdentifiers;
	private boolean storesUpperCaseIdentifiers;
	private boolean storesUpperCaseQuotedIdentifiers;
	private boolean supportsSchemasInDDL;
	private boolean supportsSchemasInDML;
	
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
		
		for (String word: SQL_92_RESERVED_WORDS)
		{
			this.reservedIdentifierSet.add(this.normalizeCase(word));
		}
		
		for (String word: metaData.getSQLKeywords().split(Strings.COMMA))
		{
			this.reservedIdentifierSet.add(this.normalizeCase(word));
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
		
		ResultSet resultSet = metaData.getColumns(this.getCatalog(metaData), this.getSchema(table), table.getName(), Strings.ANY);
		
		while (resultSet.next())
		{
			String column = this.quote(resultSet.getString("COLUMN_NAME"));
			int type = resultSet.getInt("DATA_TYPE");
			String nativeType = resultSet.getString("TYPE_NAME");
			String defaultValue = resultSet.getString("COLUMN_DEF");
			String remarks = resultSet.getString("REMARKS");
			Boolean autoIncrement = null;
			
			try
			{
				String value = resultSet.getString("IS_AUTOINCREMENT");
				
				if (value.equals("YES"))
				{
					autoIncrement = true;
				}
				else if (value.equals("NO"))
				{
					autoIncrement = false;
				}
			}
			catch (SQLException e)
			{
				// Ignore - this column is new to Java 1.6
			}
			
			columnMap.put(column, new ColumnPropertiesImpl(column, type, nativeType, defaultValue, remarks, autoIncrement));
		}
		
		resultSet.close();
		
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
		
		String normal = this.normalizeCase(raw);
		
		// Quote reserved identifiers
		boolean requiresQuoting = this.reservedIdentifierSet.contains(normal);
		
		// Quote identifiers containing special characters
		requiresQuoting |= !this.identifierPattern.matcher(raw).matches();
		
		// Quote mixed-case identifiers if detected and supported by DBMS
		requiresQuoting |= !this.supportsMixedCaseIdentifiers && this.supportsMixedCaseQuotedIdentifiers && ((this.storesLowerCaseIdentifiers && !this.storesLowerCaseQuotedIdentifiers && UPPER_CASE_PATTERN.matcher(raw).find()) || (this.storesUpperCaseIdentifiers && !this.storesUpperCaseQuotedIdentifiers && LOWER_CASE_PATTERN.matcher(raw).find()));
		
		return requiresQuoting ? this.quote + this.normalizeCaseQuoted(raw) + this.quote : normal;
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
		Collection<QualifiedName> sequences = this.dialect.getSequences(metaData);
		
		List<SequenceProperties> sequenceList = new ArrayList<SequenceProperties>(sequences.size());
		
		for (QualifiedName sequence: sequences)
		{
			sequenceList.add(new SequencePropertiesImpl(this.qualifyNameForDML(sequence)));
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
			throw new SQLException(Messages.getMessage(Messages.SCHEMA_LOOKUP_FAILED, name, defaultSchemaList, this.dialect.getClass().getName() + ".getDefaultSchemas()"));
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
			Boolean autoIncrement = column.isAutoIncrement();
			
			// Database meta data may have already identified column as identity, if not ask dialect.
			if ((autoIncrement != null) ? autoIncrement : this.dialect.isIdentity(column))
			{
				columnList.add(column.getName());
			}
		}
		
		return columnList;
	}
}
