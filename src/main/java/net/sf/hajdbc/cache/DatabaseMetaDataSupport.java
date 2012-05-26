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
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.SequenceProperties;
import net.sf.hajdbc.UniqueConstraint;


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
	Collection<QualifiedName> getTables(DatabaseMetaData metaData) throws SQLException;

	/**
	 * Returns the columns of the specified table.
	 * @param metaData a DatabaseMetaData implementation
	 * @param table a schema qualified table name
	 * @return a Map of column name to column properties
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	Map<String, ColumnProperties> getColumns(DatabaseMetaData metaData, QualifiedName table) throws SQLException;

	/**
	 * Returns the primary key of the specified table.
	 * @param metaData a DatabaseMetaData implementation
	 * @param table a schema qualified table name
	 * @return a unique constraint
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	UniqueConstraint getPrimaryKey(DatabaseMetaData metaData, QualifiedName table) throws SQLException;

	/**
	 * Returns the foreign key constraints on the specified table.
	 * @param metaData a DatabaseMetaData implementation
	 * @param table a schema qualified table name
	 * @return a Collection of foreign key constraints.
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	Collection<ForeignKeyConstraint> getForeignKeyConstraints(DatabaseMetaData metaData, QualifiedName table) throws SQLException;

	/**
	 * Returns the unique constraints on the specified table - excluding the primary key of the table.
	 * @param metaData a schema qualified table name
	 * @param table a qualified table name
	 * @param primaryKey the primary key of this table
	 * @return a Collection of unique constraints.
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	Collection<UniqueConstraint> getUniqueConstraints(DatabaseMetaData metaData, QualifiedName table, UniqueConstraint primaryKey) throws SQLException;

	/**
	 * Returns the schema qualified name of the specified table suitable for use in a data modification language (DML) statement.
	 * @param name a schema qualified name
	 * @return a Collection of unique constraints.
	 */
//	String qualifyNameForDML(QualifiedName name);

	/**
	 * Returns the schema qualified name of the specified table suitable for use in a data definition language (DDL) statement.
	 * @param name a schema qualified name
	 * @return a Collection of unique constraints.
	 */
//	String qualifyNameForDDL(QualifiedName name);
	
//	QName qualify(String name);
	
//	QName qualify(String schema, String name);
	
	/**
	 * Returns a collection of sequences using dialect specific logic.
	 * @param metaData database meta data
	 * @return a collection of sequences
	 * @throws SQLException
	 */
	Collection<SequenceProperties> getSequences(DatabaseMetaData metaData) throws SQLException;
	
	/**
	 * Locates an object from a map keyed by schema qualified name.
	 * @param <T> an object
	 * @param map a map of database 
	 * @param name the name of the object to locate
	 * @param defaultSchemaList a list of default schemas
	 * @return the object with the specified name
	 * @throws SQLException
	 */
	<T> T find(Map<QualifiedName, T> map, String name, List<String> defaultSchemaList) throws SQLException;
	
	/**
	 * Identifies any identity columns from the from the specified collection of columns
	 * @param columns the columns of a table
	 * @return a collection of column names
	 * @throws SQLException
	 */
	Collection<String> getIdentityColumns(Collection<ColumnProperties> columns) throws SQLException;
	
	Map<Integer, Map.Entry<String, Integer>> getTypes(DatabaseMetaData metaData) throws SQLException;
}
