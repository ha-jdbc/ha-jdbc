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

import java.sql.SQLException;
import java.util.List;

/**
 * Encapsulates database vendor specific SQL syntax.  
 * 
 * @author  Paul Ferraro
 * @since   1.1
 */
public interface Dialect
{
	/**
	 * Returns a simple SQL statement used to validate whether a database is alive or not.
	 * @return a SQL statement
	 */
	public String getSimpleSQL();
	
	/**
	 * Returns a SQL statement to be executed within a running transaction that will effectively lock the specified table for writing.
	 * @param metaData a cache of <code>DatabaseMetaData</code> for a given database.
	 * @param schema the name of a database schema, or null, if this database does not support schemas.
	 * @param table the name of a database table.
	 * @return a SQL statement
	 * @throws SQLException if there was an error fetching meta data.
	 */
	public String getLockTableSQL(TableProperties properties) throws SQLException;
	
	/**
	 * Returns a SQL statement used to truncate a table.
	 * @param metaData a cache of <code>DatabaseMetaData</code> for a given database.
	 * @param schema the name of a database schema, or null, if this database does not support schemas.
	 * @param table the name of a database table.
	 * @return a SQL statement
	 * @throws SQLException if there was an error fetching meta data.
	 */
	public String getTruncateTableSQL(TableProperties properties) throws SQLException;
	
	/**
	 * Returns a SQL statement used to create a foreign key constraint.
	 * @param metaData a cache of <code>DatabaseMetaData</code> for a given database.
	 * @param name the name of a foreign key constraint
	 * @param schema the name of a database schema, or null, if this database does not support schemas.
	 * @param table the name of a database table.
	 * @param column the name of a column in the table
	 * @param foreignSchema the name of a database schema, or null, if this database does not support schemas.
	 * @param foreignTable the name of a database table.
	 * @param foreignColumn the name of a column in the foreign table
	 * @return a SQL statement
	 * @throws SQLException if there was an error fetching meta data.
	 */
	public String getCreateForeignKeyConstraintSQL(ForeignKeyConstraint foreignKeyConstraint) throws SQLException;

	/**
	 * Returns a SQL statement used to drop a foreign key constraint.
	 * @param metaData a cache of <code>DatabaseMetaData</code> for a given database.
	 * @param name the name of the foreign key constraint
	 * @param schema the name of a database schema, or null, if this database does not support schemas.
	 * @param table the name of a database table.
	 * @return a SQL statement
	 * @throws SQLException if there was an error fetching meta data.
	 */
	public String getDropForeignKeyConstraintSQL(ForeignKeyConstraint constraint) throws SQLException;

	/**
	 * Returns a SQL statement used to create a unique constraint.
	 * @param metaData a cache of <code>DatabaseMetaData</code> for a given database.
	 * @param name the name of a unique key constraint
	 * @param schema the name of a database schema, or null, if this database does not support schemas.
	 * @param table the name of a database table.
	 * @param columnList a List<String> fo column names.
	 * @return a SQL statement
	 * @throws SQLException if there was an error fetching meta data.
	 */
	public String getCreateUniqueConstraintSQL(UniqueConstraint constraint) throws SQLException;

	/**
	 * Returns a SQL statement used to drop a unique constraint.
	 * @param metaData a cache of <code>DatabaseMetaData</code> for a given database.
	 * @param name the name of a unique key constraint
	 * @param schema the name of a database schema, or null, if this database does not support schemas.
	 * @param table the name of a database table.
	 * @return a SQL statement
	 * @throws SQLException if there was an error fetching meta data.
	 */
	public String getDropUniqueConstraintSQL(UniqueConstraint constraint) throws SQLException;
	
	/**
	 * Returns a SQL statement used to create a unique constraint.
	 * @param metaData a cache of <code>DatabaseMetaData</code> for a given database.
	 * @param name the name of a unique key constraint
	 * @param schema the name of a database schema, or null, if this database does not support schemas.
	 * @param table the name of a database table.
	 * @param columnList a List<String> fo column names.
	 * @return a SQL statement
	 * @throws SQLException if there was an error fetching meta data.
	 */
	public String getCreatePrimaryKeyConstraintSQL(UniqueConstraint constraint) throws SQLException;

	/**
	 * Returns a SQL statement used to drop a unique constraint.
	 * @param metaData a cache of <code>DatabaseMetaData</code> for a given database.
	 * @param name the name of a unique key constraint
	 * @param schema the name of a database schema, or null, if this database does not support schemas.
	 * @param table the name of a database table.
	 * @return a SQL statement
	 * @throws SQLException if there was an error fetching meta data.
	 */
	public String getDropPrimaryKeyConstraintSQL(UniqueConstraint constraint) throws SQLException;
	
	/**
	 * Determines whether the specified SQL is a SELECT ... FOR UPDATE statement
	 * @param metaData a cache of <code>DatabaseMetaData</code> for a given database.
	 * @param sql a SQL statement
	 * @return true if this is a SELECT ... FOR UPDATE statement, false if it is not or if SELECT...FOR UPDATE is not supported
	 * @throws SQLException if there was an error fetching meta data.
	 */
	public boolean isSelectForUpdate(String sql) throws SQLException;
	
	/**
	 * Returns the data type of the specified column of the specified schema and table
	 * @param metaData a cache of <code>DatabaseMetaData</code> for a given database.
	 * @param schema a schema name
	 * @param table a table name
	 * @param column a column name
	 * @return the JDBC data type of this column
	 */
	public int getColumnType(ColumnProperties properties) throws SQLException;
	
	/**
	 * Parses a table name from the specified INSERT SQL statement.
	 * @param sql a SQL statement
	 * @return the name of a table, or null if this SQL statement is not an INSERT statement
	 * @throws SQLException
	 * @since 1.2
	 */
//	public String parseInsertTable(String sql) throws SQLException;
	
	/**
	 * Parses a sequence name from the specified SQL statement.
	 * @param sql a SQL statement
	 * @return the name of a sequence, or null if this SQL statement does not reference a sequence
	 * @throws SQLException
	 * @since 1.2
	 */
	public String parseSequence(String sql) throws SQLException;
	
	public List<String> getSequences() throws SQLException;
}
