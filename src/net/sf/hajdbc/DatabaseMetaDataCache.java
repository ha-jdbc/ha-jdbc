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
package net.sf.hajdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

import net.sf.hajdbc.cache.ColumnProperties;


/**
 * Interface for retrieving pre-processed, and potentially cached, database meta data.
 * 
 * @author Paul Ferraro
 * @since 1.2
 */
public interface DatabaseMetaDataCache
{
	/**
	 * Initializes/Flushes this cache.
	 * @throws SQLException if flush fails
	 */
	public void flush() throws SQLException;
	
	/**
	 * Sets the connection from which this cache can reference DatabaseMetaData
	 * @param connection a database connection
	 */
	public void setConnection(Connection connection);
	
	/**
	 * Returns all tables in the databasea.
	 * @return a map of schema name to collection of table names.
	 * @throws SQLException if DatabaseMetaData access fails
	 */
	public Map<String, Collection<String>> getTables() throws SQLException;
	
	/**
	 * Returns the primary key of the specified table.
	 * @param schema a schema name, or null if database does not support schemas
	 * @param table a table name
	 * @return a primary key
	 * @throws SQLException if DatabaseMetaData access fails
	 */
	public UniqueConstraint getPrimaryKey(String schema, String table) throws SQLException;
	
	/**
	 * Returns the foreign keys of the specified table
	 * @param schema a schema name, or null if database does not support schemas
	 * @param table a table name
	 * @return a collection of foreign keys
	 * @throws SQLException if DatabaseMetaData access fails
	 */
	public Collection<ForeignKeyConstraint> getForeignKeyConstraints(String schema, String table) throws SQLException;

	/**
	 * Returns the unique constraints of the specified table.  This may include primary keys.
	 * @param schema a schema name, or null if database does not support schemas
	 * @param table a table name
	 * @return a collection of unique constraints
	 * @throws SQLException if DatabaseMetaData access fails
	 */
	public Collection<UniqueConstraint> getUniqueConstraints(String schema, String table) throws SQLException;
	
	/**
	 * Returns the columns of the specified table.
	 * @param schema a schema name, or null if database does not support schemas
	 * @param table a table name
	 * @return a map of column name to properties
	 * @throws SQLException if DatabaseMetaData access fails
	 */
	public Map<String, ColumnProperties> getColumns(String schema, String table) throws SQLException;
	
	/**
	 * Returns the schema qualified name of the specified table as appropriate for data definition language (DDL) statements.
	 * @param schema a schema name, or null if database does not support schemas
	 * @param table a table name
	 * @return a qualified table name
	 * @throws SQLException if DatabaseMetaData access fails
	 */
	public String getQualifiedNameForDDL(String schema, String table) throws SQLException;

	/**
	 * Returns the schema qualified name of the specified table as appropriate for data modification language (DML) statements.
	 * @param schema a schema name, or null if database does not support schemas
	 * @param table a table name
	 * @return a qualified table name
	 * @throws SQLException if DatabaseMetaData access fails
	 */
	public String getQualifiedNameForDML(String schema, String table) throws SQLException;

	/**
	 * Indicates whether or not this database support SELECT...FOR UPDATE statements.
	 * @return true, if SELECT...FOR UPDATE is supported, false otherwise
	 * @throws SQLException if DatabaseMetaData access fails
	 */
	public boolean supportsSelectForUpdate() throws SQLException;
	
//	public boolean containsAutoIncrementColumn(String qualifiedTable) throws SQLException;
}