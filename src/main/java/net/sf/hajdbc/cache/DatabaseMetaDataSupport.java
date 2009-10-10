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
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;


/**
 * Processes database meta data into useful structures.
 * @author Paul Ferraro
 */
public interface DatabaseMetaDataSupport
{
	/**
	 * Returns all tables in this database mapped by schema.
	 * @param metaData a DatabaseMetaData implementation
	 * @return a Map of schema name to Collection of table names
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	public Collection<QualifiedName> getTables(DatabaseMetaData metaData) throws SQLException;

	/**
	 * Returns the columns of the specified table.
	 * @param metaData a DatabaseMetaData implementation
	 * @param table a schema qualified table name
	 * @return a Map of column name to column properties
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	public Map<String, ColumnProperties> getColumns(DatabaseMetaData metaData, QualifiedName table) throws SQLException;

	/**
	 * Returns the primary key of the specified table.
	 * @param metaData a DatabaseMetaData implementation
	 * @param table a schema qualified table name
	 * @return a unique constraint
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	public UniqueConstraint getPrimaryKey(DatabaseMetaData metaData, QualifiedName table) throws SQLException;

	/**
	 * Returns the foreign key constraints on the specified table.
	 * @param metaData a DatabaseMetaData implementation
	 * @param table a schema qualified table name
	 * @return a Collection of foreign key constraints.
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	public Collection<ForeignKeyConstraint> getForeignKeyConstraints(DatabaseMetaData metaData, QualifiedName table) throws SQLException;

	/**
	 * Returns the unique constraints on the specified table - excluding the primary key of the table.
	 * @param metaData a schema qualified table name
	 * @param table a qualified table name
	 * @param primaryKey the primary key of this table
	 * @return a Collection of unique constraints.
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	public Collection<UniqueConstraint> getUniqueConstraints(DatabaseMetaData metaData, QualifiedName table, UniqueConstraint primaryKey) throws SQLException;

	/**
	 * Returns the schema qualified name of the specified table suitable for use in a data modification language (DML) statement.
	 * @param name a schema qualified name
	 * @return a Collection of unique constraints.
	 */
	public String qualifyNameForDML(QualifiedName name);

	/**
	 * Returns the schema qualified name of the specified table suitable for use in a data definition language (DDL) statement.
	 * @param name a schema qualified name
	 * @return a Collection of unique constraints.
	 */
	public String qualifyNameForDDL(QualifiedName name);
	
	/**
	 * Returns a collection of sequences using dialect specific logic.
	 * @param metaData database meta data
	 * @return a collection of sequences
	 * @throws SQLException
	 */
	public Collection<SequenceProperties> getSequences(DatabaseMetaData metaData) throws SQLException;
	
	/**
	 * Locates an object from a map keyed by schema qualified name.
	 * @param <T> an object
	 * @param map a map of database 
	 * @param name the name of the object to locate
	 * @param defaultSchemaList a list of default schemas
	 * @return the object with the specified name
	 * @throws SQLException
	 */
	public <T> T find(Map<String, T> map, String name, List<String> defaultSchemaList) throws SQLException;
	
	/**
	 * Identifies any identity columns from the from the specified collection of columns
	 * @param columns the columns of a table
	 * @return a collection of column names
	 * @throws SQLException
	 */
	public Collection<String> getIdentityColumns(Collection<ColumnProperties> columns) throws SQLException;
}
