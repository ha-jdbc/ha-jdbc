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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.transaction.xa.XAException;

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.ColumnPropertiesFactory;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DumpRestoreSupport;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.ForeignKeyConstraintFactory;
import net.sf.hajdbc.IdentifierNormalizer;
import net.sf.hajdbc.IdentityColumnSupport;
import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.QualifiedNameFactory;
import net.sf.hajdbc.SequenceSupport;
import net.sf.hajdbc.TableProperties;
import net.sf.hajdbc.TriggerSupport;
import net.sf.hajdbc.UniqueConstraint;
import net.sf.hajdbc.UniqueConstraintFactory;
import net.sf.hajdbc.codec.Decoder;


/**
 * Encapsulates database vendor specific SQL syntax.  
 * 
 * @author  Paul Ferraro
 * @since   1.1
 */
public interface Dialect
{
	/**
	 * Returns a SQL statement used to truncate a table.
	 * @param properties table meta data
	 * @return a SQL statement
	 * @throws SQLException if there was an error fetching meta data.
	 */
	String getTruncateTableSQL(TableProperties properties) throws SQLException;
	
	/**
	 * Returns a SQL statement used to create a foreign key constraint.
	 * @param constraint foreign key constraint meta data
	 * @return a SQL statement
	 * @throws SQLException if there was an error fetching meta data.
	 */
	String getCreateForeignKeyConstraintSQL(ForeignKeyConstraint constraint) throws SQLException;

	/**
	 * Returns a SQL statement used to drop a foreign key constraint.
	 * @param constraint foreign key constraint meta data
	 * @return a SQL statement
	 * @throws SQLException if there was an error fetching meta data.
	 */
	String getDropForeignKeyConstraintSQL(ForeignKeyConstraint constraint) throws SQLException;

	/**
	 * Returns a SQL statement used to create a unique constraint.
	 * @param constraint unique constraint meta data
	 * @return a SQL statement
	 * @throws SQLException if there was an error fetching meta data.
	 */
	String getCreateUniqueConstraintSQL(UniqueConstraint constraint) throws SQLException;

	/**
	 * Returns a SQL statement used to drop a unique constraint.
	 * @param constraint unique constraint meta data
	 * @return a SQL statement
	 * @throws SQLException if there was an error fetching meta data.
	 */
	String getDropUniqueConstraintSQL(UniqueConstraint constraint) throws SQLException;
	
	/**
	 * Determines whether the specified SQL is a SELECT ... FOR UPDATE statement
	 * @param sql a SQL statement
	 * @return true if this is a SELECT ... FOR UPDATE statement, false if it is not
	 * @throws SQLException if there was an error fetching meta data.
	 */
	boolean isSelectForUpdate(String sql) throws SQLException;
	
	/**
	 * Returns the data type of the specified column of the specified schema and table.
	 * This method is intended to correct JDBC driver type mapping quirks.
	 * @param properties table column meta data
	 * @return the JDBC data type of this column
	 * @throws SQLException 
	 */
	int getColumnType(ColumnProperties properties) throws SQLException;
	
	/**
	 * Returns a search path of schemas 
	 * @param metaData 
	 * @return a list of schema names
	 * @throws SQLException 
	 * @since 2.0
	 */
	List<String> getDefaultSchemas(DatabaseMetaData metaData) throws SQLException;
	
	/**
	 * Replaces non-deterministic CURRENT_DATE functions with deterministic static values.
	 * @param sql an SQL statement
	 * @param date the replacement date
	 * @return an equivalent deterministic SQL statement
	 * @throws SQLException 
	 * @since 2.0.2
	 */
	String evaluateCurrentDate(String sql, java.sql.Date date);
	
	/**
	 * Replaces non-deterministic CURRENT_TIME functions with deterministic static values.
	 * @param sql an SQL statement
	 * @param time the replacement time
	 * @return an equivalent deterministic SQL statement
	 * @throws SQLException 
	 * @since 2.0.2
	 */
	String evaluateCurrentTime(String sql, java.sql.Time time);
	
	/**
	 * Replaces non-deterministic CURRENT_TIMESTAMP functions with deterministic static values.
	 * @param sql an SQL statement
	 * @param timestamp the replacement timestamp
	 * @return an equivalent deterministic SQL statement
	 * @throws SQLException 
	 * @since 2.0.2
	 */
	String evaluateCurrentTimestamp(String sql, java.sql.Timestamp timestamp);
	
	/**
	 * Replaces non-deterministic RAND() functions with deterministic static values.
	 * @param sql an SQL statement
	 * @return an equivalent deterministic SQL statement
	 * @throws SQLException 
	 * @since 2.0.2
	 */
	String evaluateRand(String sql);
	
	/**
	 * Determines whether the specified exception indicates a catastrophic error.
	 * @param e an exception
	 * @return true, if the exception indicates catastrophe, false otherwise
	 */
	boolean indicatesFailure(SQLException e);
	
	/**
	 * Determines whether the specified exception indicates a catastrophic error.
	 * @param e an exception
	 * @return true, if the exception indicates catastrophe, false otherwise
	 */
	boolean indicatesFailure(XAException e);

	SequenceSupport getSequenceSupport();
	
	IdentityColumnSupport getIdentityColumnSupport();
	
	DumpRestoreSupport getDumpRestoreSupport();
	
	TriggerSupport getTriggerSupport();
	
	String getCreateSchemaSQL(String schema);
	
	String getDropSchemaSQL(String schema);
	
	/**
	 * Returns all tables in this database mapped by schema.
	 * @param metaData a DatabaseMetaData implementation
	 * @return a Map of schema name to Collection of table names
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	Collection<QualifiedName> getTables(DatabaseMetaData metaData, QualifiedNameFactory factory) throws SQLException;

	/**
	 * Returns the columns of the specified table.
	 * @param metaData a DatabaseMetaData implementation
	 * @param table a schema qualified table name
	 * @return a Map of column name to column properties
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	Map<String, ColumnProperties> getColumns(DatabaseMetaData metaData, QualifiedName table, ColumnPropertiesFactory factory) throws SQLException;

	/**
	 * Returns the primary key of the specified table.
	 * @param metaData a DatabaseMetaData implementation
	 * @param table a schema qualified table name
	 * @return a unique constraint
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	UniqueConstraint getPrimaryKey(DatabaseMetaData metaData, QualifiedName table, UniqueConstraintFactory factory) throws SQLException;

	/**
	 * Returns the foreign key constraints on the specified table.
	 * @param metaData a DatabaseMetaData implementation
	 * @param table a schema qualified table name
	 * @return a Collection of foreign key constraints.
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	Collection<ForeignKeyConstraint> getForeignKeyConstraints(DatabaseMetaData metaData, QualifiedName table, ForeignKeyConstraintFactory factory) throws SQLException;

	/**
	 * Returns the unique constraints on the specified table - excluding the primary key of the table.
	 * @param metaData a schema qualified table name
	 * @param table a qualified table name
	 * @param primaryKey the primary key of this table
	 * @return a Collection of unique constraints.
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	Collection<UniqueConstraint> getUniqueConstraints(DatabaseMetaData metaData, QualifiedName table, UniqueConstraint primaryKey, UniqueConstraintFactory factory) throws SQLException;
	
	/**
	 * Identifies any identity columns from the from the specified collection of columns
	 * @param columns the columns of a table
	 * @return a collection of column names
	 * @throws SQLException
	 */
	Collection<String> getIdentityColumns(Collection<ColumnProperties> columns) throws SQLException;
	
	/**
	 * Returns a mapping of standard JDBC types to native types
	 * @param metaData database meta data
	 * @return a map of JDBC types
	 * @throws SQLException
	 */
	Map<Integer, Map.Entry<String, Integer>> getTypes(DatabaseMetaData metaData) throws SQLException;

	IdentifierNormalizer createIdentifierNormalizer(DatabaseMetaData metaData) throws SQLException;
	
	QualifiedNameFactory createQualifiedNameFactory(DatabaseMetaData metaData, IdentifierNormalizer normalizer) throws SQLException;
	
	ColumnPropertiesFactory createColumnPropertiesFactory(IdentifierNormalizer normalizer);
	
	ForeignKeyConstraintFactory createForeignKeyConstraintFactory(QualifiedNameFactory factory);
	
	UniqueConstraintFactory createUniqueConstraintFactory(IdentifierNormalizer normalizer);
	
	boolean isValid(Connection connection) throws SQLException;
	
	<Z, D extends Database<Z>> ConnectionProperties getConnectionProperties(D database, Decoder decoder) throws SQLException;
}
