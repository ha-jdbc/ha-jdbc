/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
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
import java.util.Arrays;
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
import net.sf.hajdbc.TableProperties;
import net.sf.hajdbc.UniqueConstraint;

/**
 * Processes database meta data into useful structures.
 * @author Paul Ferraro
 */
public class DatabaseMetaDataSupport
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
	
	private Set<String> reservedIdentifierSet;
	private Pattern identifierPattern;
	private String quote;
	private boolean supportsMixedCaseIdentifiers;
	private boolean supportsMixedCaseQuotedIdentifiers;
	private boolean storesLowerCaseIdentifiers;
	private boolean storesUpperCaseIdentifiers;
	private boolean supportsSchemasInDDL;
	private boolean supportsSchemasInDML;
	
	/**
	 * Constructs a new DatabaseMetaDataSupport using the specified DatabaseMetaData implementation.
	 * @param metaData a DatabaseMetaData implementation
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	public DatabaseMetaDataSupport(DatabaseMetaData metaData) throws SQLException
	{
		this.reservedIdentifierSet = new HashSet<String>(Arrays.asList(SQL_92_RESERVED_WORDS));
		this.reservedIdentifierSet.addAll(Arrays.asList(metaData.getSQLKeywords().split(",")));
		
		this.identifierPattern = Pattern.compile("[\\w" + Pattern.quote(metaData.getExtraNameCharacters()) + "]+");
		this.quote = metaData.getIdentifierQuoteString();
		this.supportsMixedCaseIdentifiers = metaData.supportsMixedCaseIdentifiers();
		this.supportsMixedCaseQuotedIdentifiers = metaData.supportsMixedCaseQuotedIdentifiers();
		this.storesLowerCaseIdentifiers = metaData.storesLowerCaseIdentifiers();
		this.storesUpperCaseIdentifiers = metaData.storesUpperCaseIdentifiers();
		this.supportsSchemasInDML = metaData.supportsSchemasInDataManipulation();
		this.supportsSchemasInDDL = metaData.supportsSchemasInTableDefinitions();
	}
	
	/**
	 * Returns all tables in this database mapped by schema.
	 * @param metaData a DatabaseMetaData implementation
	 * @return a Map of schema name to Collection of table names
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	public Map<String, Collection<String>> getTables(DatabaseMetaData metaData) throws SQLException
	{
		Map<String, Collection<String>> tablesMap = new HashMap<String, Collection<String>>();
		
		ResultSet resultSet = metaData.getTables(this.getCatalog(metaData), null, "%", new String[] { "TABLE" });
		
		while (resultSet.next())
		{
			String table = resultSet.getString("TABLE_NAME");
			String schema = resultSet.getString("TABLE_SCHEM");

			Collection<String> tables = tablesMap.get(schema);
			
			if (tables == null)
			{
				tables = new LinkedList<String>();
				
				tablesMap.put(schema, tables);
			}
			
			tables.add(table);
		}
		
		resultSet.close();
		
		return tablesMap;
	}

	/**
	 * Returns the columns of the specified table.
	 * @param metaData a DatabaseMetaData implementation
	 * @param schema a schema name, possibly null
	 * @param table a table name
	 * @return a Map of column name to column properties
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	public Map<String, ColumnProperties> getColumns(DatabaseMetaData metaData, String schema, String table) throws SQLException
	{
		Map<String, ColumnProperties> columnMap = new HashMap<String, ColumnProperties>();
		
		ResultSet resultSet = metaData.getColumns(this.getCatalog(metaData), this.getSchema(schema), table, "%");
		
		while (resultSet.next())
		{
			String column = this.quote(resultSet.getString("COLUMN_NAME"));
			int type = resultSet.getInt("DATA_TYPE");
			String nativeType = resultSet.getString("TYPE_NAME");
			String remarks = resultSet.getString("REMARKS");
			
			columnMap.put(column, new ColumnPropertiesImpl(column, type, nativeType, remarks));
		}
		
		resultSet.close();
		
		return columnMap;
	}

	/**
	 * Returns the primary key of the specified table.
	 * @param metaData a DatabaseMetaData implementation
	 * @param schema a schema name, possibly null
	 * @param table a table name
	 * @return a unique constraint
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	public UniqueConstraint getPrimaryKey(DatabaseMetaData metaData, String schema, String table) throws SQLException
	{
		UniqueConstraint constraint = null;

		ResultSet resultSet = metaData.getPrimaryKeys(this.getCatalog(metaData), this.getSchema(schema), table);
		
		while (resultSet.next())
		{
			String name = this.quote(resultSet.getString("PK_NAME"));

			if (constraint == null)
			{
				constraint = new UniqueConstraintImpl(name, this.getQualifiedNameForDDL(schema, table));
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
	 * @param schema a schema name, possibly null
	 * @param table a table name
	 * @return a Collection of foreign key constraints.
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	public Collection<ForeignKeyConstraint> getForeignKeyConstraints(DatabaseMetaData metaData, String schema, String table) throws SQLException
	{
		Map<String, ForeignKeyConstraint> foreignKeyMap = new HashMap<String, ForeignKeyConstraint>();
		
		ResultSet resultSet = metaData.getImportedKeys(this.getCatalog(metaData), this.getSchema(schema), table);
		
		while (resultSet.next())
		{
			String name = this.quote(resultSet.getString("FK_NAME"));
			
			ForeignKeyConstraint foreignKey = foreignKeyMap.get(name);
			
			if (foreignKey == null)
			{
				foreignKey = new ForeignKeyConstraintImpl(name, this.getQualifiedNameForDDL(schema, table));
				
				String foreignSchema = this.quote(resultSet.getString("PKTABLE_SCHEM"));
				String foreignTable = this.quote(resultSet.getString("PKTABLE_NAME"));
				
				foreignKey.setForeignTable(this.getQualifiedNameForDDL(foreignSchema, foreignTable));
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
	 * Returns the unique constraints on the specified table.  This may include the primary key of the table.
	 * @param metaData a DatabaseMetaData implementation
	 * @param schema a schema name, possibly null
	 * @param table a table name
	 * @return a Collection of unique constraints.
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	public Collection<UniqueConstraint> getUniqueConstraints(DatabaseMetaData metaData, String schema, String table) throws SQLException
	{
		Map<String, UniqueConstraint> keyMap = new HashMap<String, UniqueConstraint>();
		
		ResultSet resultSet = metaData.getIndexInfo(this.getCatalog(metaData), this.getSchema(schema), table, true, false);
		
		while (resultSet.next())
		{
			if (resultSet.getInt("TYPE") == DatabaseMetaData.tableIndexStatistic) continue;
			
			String name = this.quote(resultSet.getString("INDEX_NAME"));
			
			UniqueConstraint key = keyMap.get(name);
			
			if (key == null)
			{
				key = new UniqueConstraintImpl(name, this.getQualifiedNameForDDL(schema, table));
				
				keyMap.put(name, key);
			}
			
			String column = resultSet.getString("COLUMN_NAME");
			
			key.getColumnList().add(column);
		}
		
		resultSet.close();
		
		return keyMap.values();
	}

	/**
	 * Returns the schema qualified name of the specified table suitable for use in a data modification language (DML) statement.
	 * @param schema a schema name, possibly null
	 * @param table a table name
	 * @return a Collection of unique constraints.
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	public String getQualifiedNameForDML(String schema, String table)
	{
		StringBuilder builder = new StringBuilder();
		
		if (this.supportsSchemasInDML && (schema != null))
		{
			builder.append(this.quote(schema)).append(".");
		}
		
		return builder.append(this.quote(table)).toString();
	}

	/**
	 * Returns the schema qualified name of the specified table suitable for use in a data definition language (DDL) statement.
	 * @param schema a schema name, possibly null
	 * @param table a table name
	 * @return a Collection of unique constraints.
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	public String getQualifiedNameForDDL(String schema, String table)
	{
		StringBuilder builder = new StringBuilder();
		
		if (this.supportsSchemasInDDL && (schema != null))
		{
			builder.append(this.quote(schema)).append(".");
		}
		
		return builder.append(this.quote(table)).toString();
	}

	private String getCatalog(DatabaseMetaData metaData) throws SQLException
	{
		String catalog = metaData.getConnection().getCatalog();
		
		return (catalog != null) ? catalog : "";
	}
	
	private String getSchema(String schema)
	{
		return (schema != null) ? schema : "";
	}
	
	private String quote(String identifier)
	{
		if (identifier == null) return null;
		
		// Driver may return identifiers already quoted.  If so, exit early.
		if (identifier.startsWith(this.quote)) return identifier;
		
		// Quote reserved identifiers
		boolean requiresQuoting = this.reservedIdentifierSet.contains(identifier.toLowerCase());
		
		// Quote identifers containing special characters
		requiresQuoting |= !this.identifierPattern.matcher(identifier).matches();
		
		// Quote mixed-case identifers if detected and supported by DBMS
		requiresQuoting |= !this.supportsMixedCaseIdentifiers && this.supportsMixedCaseQuotedIdentifiers && ((this.storesLowerCaseIdentifiers && UPPER_CASE_PATTERN.matcher(identifier).find()) || (this.storesUpperCaseIdentifiers && LOWER_CASE_PATTERN.matcher(identifier).find()));
		
		return requiresQuoting ? this.quote + identifier + this.quote : identifier;
	}
	
	private String normalize(String tableName, String defaultSchema)
	{
		String parts[] = tableName.split("\\.");

		String table = parts[parts.length - 1];
		String schema = (parts.length > 1) ? parts[parts.length - 2] : defaultSchema;

		return this.getQualifiedNameForDML(schema, table);
	}
	
	public TableProperties findTable(Map<String, TableProperties> tableMap, String table, List<String> defaultSchemaList, Class<? extends Dialect> dialectClass) throws SQLException
	{
		TableProperties properties = tableMap.get(this.normalize(table, null));
		
		if (properties == null)
		{
			for (String schema: defaultSchemaList)
			{
				if (properties == null)
				{
					properties = tableMap.get(this.normalize(table, schema));
				}
			}
		}
		
		if (properties == null)
		{
			throw new SQLException(Messages.getMessage(Messages.TABLE_LOOKUP_FAILED, table, defaultSchemaList, dialectClass.getName() + ".getDefaultSchemas()"));
		}
		
		return properties;
	}
}
