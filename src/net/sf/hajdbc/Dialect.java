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
package net.sf.hajdbc;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

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
	public String getSimpleSQL() throws SQLException;
	
	/**
	 * Returns a SQL statement to be executed within a running transaction that will effectively lock the specified table for writing.
	 * @param properties table meta data
	 * @return a SQL statement
	 * @throws SQLException if there was an error fetching meta data.
	 */
	public String getLockTableSQL(TableProperties properties) throws SQLException;
	
	/**
	 * Returns a SQL statement used to truncate a table.
	 * @param properties table meta data
	 * @return a SQL statement
	 * @throws SQLException if there was an error fetching meta data.
	 */
	public String getTruncateTableSQL(TableProperties properties) throws SQLException;
	
	/**
	 * Returns a SQL statement used to create a foreign key constraint.
	 * @param constraint foreign key constraint meta data
	 * @return a SQL statement
	 * @throws SQLException if there was an error fetching meta data.
	 */
	public String getCreateForeignKeyConstraintSQL(ForeignKeyConstraint constraint) throws SQLException;

	/**
	 * Returns a SQL statement used to drop a foreign key constraint.
	 * @param constraint foreign key constraint meta data
	 * @return a SQL statement
	 * @throws SQLException if there was an error fetching meta data.
	 */
	public String getDropForeignKeyConstraintSQL(ForeignKeyConstraint constraint) throws SQLException;

	/**
	 * Returns a SQL statement used to create a unique constraint.
	 * @param constraint unique constraint meta data
	 * @return a SQL statement
	 * @throws SQLException if there was an error fetching meta data.
	 */
	public String getCreateUniqueConstraintSQL(UniqueConstraint constraint) throws SQLException;

	/**
	 * Returns a SQL statement used to drop a unique constraint.
	 * @param constraint unique constraint meta data
	 * @return a SQL statement
	 * @throws SQLException if there was an error fetching meta data.
	 */
	public String getDropUniqueConstraintSQL(UniqueConstraint constraint) throws SQLException;
	
	/**
	 * Determines whether the specified SQL is a SELECT ... FOR UPDATE statement
	 * @param sql a SQL statement
	 * @return true if this is a SELECT ... FOR UPDATE statement, false if it is not
	 * @throws SQLException if there was an error fetching meta data.
	 */
	public boolean isSelectForUpdate(String sql) throws SQLException;
	
	/**
	 * Returns the data type of the specified column of the specified schema and table.
	 * This method is intended to correct JDBC driver type mapping flaws.
	 * @param properties table column meta data
	 * @return the JDBC data type of this column
	 */
	public int getColumnType(ColumnProperties properties) throws SQLException;
	
	/**
	 * Parses a table name from the specified INSERT SQL statement that may contain identity columns.
	 * @param sql a SQL statement
	 * @return the name of a table, or null if this SQL statement is not an INSERT statement or this dialect does not support identity columns
	 * @throws SQLException
	 * @since 2.0
	 */
	public String parseInsertTable(String sql) throws SQLException;
	
	/**
	 * Indicates whether or not the specified column is an identity column.
	 * @param properties a table column
	 * @return true, if this column is an identity column, false otherwise
	 * @throws SQLException
	 * @since 2.0
	 */
	public boolean isIdentity(ColumnProperties properties) throws SQLException;

	/**
	 * Parses a sequence name from the specified SQL statement.
	 * @param sql a SQL statement
	 * @return the name of a sequence, or null if this SQL statement does not reference a sequence or this dialect does not support sequences
	 * @throws SQLException
	 * @since 2.0
	 */
	public String parseSequence(String sql) throws SQLException;
	
	/**
	 * Returns a collection of all sequences in this database.
	 * @param connection a database connection
	 * @return a Map of sequence name to current value
	 * @throws SQLException
	 * @since 2.0
	 */
	public Collection<QualifiedName> getSequences(DatabaseMetaData metaData) throws SQLException;
	
	/**
	 * Returns a SQL statement for obtaining the next value the specified sequence
	 * @param sequence a sequence name
	 * @return a SQL statement
	 * @throws SQLException
	 * @since 2.0
	 */
	public String getNextSequenceValueSQL(SequenceProperties sequence) throws SQLException;

	/**
	 * Returns a SQL statement used reset the current value of a sequence.
	 * @param sequence a sequence name
	 * @param value a sequence value
	 * @return a SQL statement
	 * @since 2.0
	 */
	public String getAlterSequenceSQL(SequenceProperties sequence, long value) throws SQLException;
	
	/**
	 * Returns a SQL statement used reset the current value of an identity column.
	 * @param table a sequence name
	 * @param column a sequence name
	 * @param value a sequence value
	 * @return a SQL statement
	 * @since 2.0.2
	 */
	public String getAlterIdentityColumnSQL(TableProperties table, ColumnProperties column, long value) throws SQLException;
	
	/**
	 * Returns a search path of schemas 
	 * @return a list of schema names
	 * @since 2.0
	 */
	public List<String> getDefaultSchemas(DatabaseMetaData metaData) throws SQLException;
	
	/**
	 * Returns a pattern for identifiers that do not require quoting
	 * @return a regular expression pattern
	 * @since 2.0.2
	 */
	public Pattern getIdentifierPattern(DatabaseMetaData metaData) throws SQLException;
	
	/**
	 * Replaces non-deterministic CURRENT_DATE functions with deterministic static values.
	 * @param sql an SQL statement
	 * @param date the replacement date
	 * @return an equivalent deterministic SQL statement
	 * @since 2.0.2
	 */
	public String evaluateCurrentDate(String sql, java.sql.Date date);
	
	/**
	 * Replaces non-deterministic CURRENT_TIME functions with deterministic static values.
	 * @param sql an SQL statement
	 * @param time the replacement time
	 * @return an equivalent deterministic SQL statement
	 * @since 2.0.2
	 */
	public String evaluateCurrentTime(String sql, java.sql.Time time);
	
	/**
	 * Replaces non-deterministic CURRENT_TIMESTAMP functions with deterministic static values.
	 * @param sql an SQL statement
	 * @param timestamp the replacement timestamp
	 * @return an equivalent deterministic SQL statement
	 * @since 2.0.2
	 */
	public String evaluateCurrentTimestamp(String sql, java.sql.Timestamp timestamp);
	
	/**
	 * Replaces non-deterministic RAND() functions with deterministic static values.
	 * @param sql an SQL statement
	 * @return an equivalent deterministic SQL statement
	 * @since 2.0.2
	 */
	public String evaluateRand(String sql);
}
