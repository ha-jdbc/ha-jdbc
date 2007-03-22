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
package net.sf.hajdbc.sync;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseMetaDataCache;
import net.sf.hajdbc.DatabaseProperties;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.SynchronizationContext;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.TableProperties;

import org.easymock.EasyMock;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
public class TestFullSynchronizationStrategy extends TestLockingSynchronizationStrategy
{
	/**
	 * @see net.sf.hajdbc.sync.TestLockingSynchronizationStrategy#createSynchronizationStrategy()
	 */
	@Override
	protected SynchronizationStrategy createSynchronizationStrategy()
	{
		return new FullSynchronizationStrategy();
	}

	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#synchronize(java.sql.Connection, java.sql.Connection, net.sf.hajdbc.DatabaseMetaDataCache, net.sf.hajdbc.Dialect)
	 */
	@SuppressWarnings("unchecked")
	@Test(dataProvider = "context")
	public <D> void synchronize(SynchronizationContext<D> context) throws SQLException
	{
		Database<D> sourceDatabase = EasyMock.createStrictMock(Database.class);
		Database<D> targetDatabase = EasyMock.createStrictMock(Database.class);
		Connection sourceConnection = EasyMock.createStrictMock(Connection.class);
		Connection targetConnection = EasyMock.createStrictMock(Connection.class);
		Statement statement = EasyMock.createStrictMock(Statement.class);
		DatabaseMetaDataCache metaData = EasyMock.createStrictMock(DatabaseMetaDataCache.class);
		DatabaseProperties database = EasyMock.createStrictMock(DatabaseProperties.class);
		TableProperties table = EasyMock.createStrictMock(TableProperties.class);
		Dialect dialect = EasyMock.createStrictMock(Dialect.class);
		Statement targetStatement = EasyMock.createStrictMock(Statement.class);
		Statement sourceStatement = EasyMock.createStrictMock(Statement.class);
		ResultSet sourceResultSet = EasyMock.createStrictMock(ResultSet.class);
		ForeignKeyConstraint foreignKey = EasyMock.createStrictMock(ForeignKeyConstraint.class);
		Statement selectStatement = EasyMock.createStrictMock(Statement.class);
		ResultSet resultSet = EasyMock.createStrictMock(ResultSet.class);
		Statement deleteStatement = EasyMock.createStrictMock(Statement.class);
		PreparedStatement insertStatement = EasyMock.createStrictMock(PreparedStatement.class);
		ColumnProperties column1 = EasyMock.createStrictMock(ColumnProperties.class);
		ColumnProperties column2 = EasyMock.createStrictMock(ColumnProperties.class);
		ExecutorService executor = Executors.newSingleThreadExecutor();
		
		EasyMock.expect(context.getSourceDatabase()).andReturn(sourceDatabase);
		EasyMock.expect(context.getConnection(sourceDatabase)).andReturn(sourceConnection);

		EasyMock.expect(context.getTargetDatabase()).andReturn(targetDatabase);
		EasyMock.expect(context.getConnection(targetDatabase)).andReturn(targetConnection);
		
		EasyMock.expect(context.getDialect()).andReturn(dialect);
		EasyMock.expect(context.getExecutor()).andReturn(executor);

		targetConnection.setAutoCommit(true);
		
		EasyMock.expect(context.getTargetDatabase()).andReturn(targetDatabase);
		EasyMock.expect(context.getConnection(targetDatabase)).andReturn(targetConnection);
		
		EasyMock.expect(context.getDatabaseMetaDataCache()).andReturn(metaData);
		EasyMock.expect(metaData.getDatabaseProperties(targetConnection)).andReturn(database);
		EasyMock.expect(database.getTables()).andReturn(Collections.singleton(table));
		EasyMock.expect(context.getDialect()).andReturn(dialect);

		EasyMock.expect(targetConnection.createStatement()).andReturn(targetStatement);
		
		EasyMock.expect(table.getForeignKeyConstraints()).andReturn(Collections.singleton(foreignKey));
		EasyMock.expect(dialect.getDropForeignKeyConstraintSQL(foreignKey)).andReturn("drop fk");
		
		targetStatement.addBatch("drop fk");

		EasyMock.expect(targetStatement.executeBatch()).andReturn(null);

		targetStatement.close();
		
		targetConnection.setAutoCommit(false);

		EasyMock.expect(context.getDatabaseMetaDataCache()).andReturn(metaData);
		EasyMock.expect(metaData.getDatabaseProperties(sourceConnection)).andReturn(database);
		EasyMock.expect(database.getTables()).andReturn(Collections.singleton(table));
		
		EasyMock.expect(table.getName()).andReturn("table");
		EasyMock.expect(table.getColumns()).andReturn(Arrays.asList(new String[] { "column1", "column2" }));
		
		EasyMock.expect(sourceConnection.createStatement()).andReturn(selectStatement);
		selectStatement.setFetchSize(0);
		
		EasyMock.checkOrder(dialect, false);
		EasyMock.checkOrder(targetConnection, false);
		EasyMock.checkOrder(deleteStatement, false);
		EasyMock.checkOrder(selectStatement, false);
		
		EasyMock.expect(dialect.getTruncateTableSQL(table)).andReturn("DELETE FROM table");
		EasyMock.expect(targetConnection.createStatement()).andReturn(deleteStatement);
		EasyMock.expect(deleteStatement.executeUpdate("DELETE FROM table")).andReturn(0);
		
		deleteStatement.close();
		
		EasyMock.expect(selectStatement.executeQuery("SELECT column1, column2 FROM table")).andReturn(resultSet);
		
		EasyMock.checkOrder(dialect, true);
		EasyMock.checkOrder(targetConnection, true);
		EasyMock.checkOrder(deleteStatement, true);
		EasyMock.checkOrder(selectStatement, true);
		
		EasyMock.expect(targetConnection.prepareStatement("INSERT INTO table (column1, column2) VALUES (?, ?)")).andReturn(insertStatement);
		
		EasyMock.expect(resultSet.next()).andReturn(true);
		
		EasyMock.expect(table.getColumnProperties("column1")).andReturn(column1);
		EasyMock.expect(dialect.getColumnType(column1)).andReturn(Types.INTEGER);
		EasyMock.expect(resultSet.getObject(1)).andReturn(1);
		EasyMock.expect(resultSet.wasNull()).andReturn(false);
		insertStatement.setObject(1, 1, Types.INTEGER);
		
		EasyMock.expect(table.getColumnProperties("column2")).andReturn(column2);
		EasyMock.expect(dialect.getColumnType(column2)).andReturn(Types.VARCHAR);
		EasyMock.expect(resultSet.getObject(2)).andReturn("");
		EasyMock.expect(resultSet.wasNull()).andReturn(false);
		insertStatement.setObject(2, "", Types.VARCHAR);
		
		insertStatement.addBatch();
		insertStatement.clearParameters();
		
		EasyMock.expect(resultSet.next()).andReturn(true);
		
		EasyMock.expect(table.getColumnProperties("column1")).andReturn(column1);
		EasyMock.expect(dialect.getColumnType(column1)).andReturn(Types.BLOB);
		EasyMock.expect(resultSet.getBlob(1)).andReturn(null);
		EasyMock.expect(resultSet.wasNull()).andReturn(true);
		insertStatement.setNull(1, Types.BLOB);
		
		EasyMock.expect(table.getColumnProperties("column2")).andReturn(column2);
		EasyMock.expect(dialect.getColumnType(column2)).andReturn(Types.CLOB);
		EasyMock.expect(resultSet.getClob(2)).andReturn(null);
		EasyMock.expect(resultSet.wasNull()).andReturn(true);
		insertStatement.setNull(2, Types.CLOB);
		
		insertStatement.addBatch();
		insertStatement.clearParameters();
		
		EasyMock.expect(resultSet.next()).andReturn(false);
		
		EasyMock.expect(insertStatement.executeBatch()).andReturn(null);
		
		insertStatement.close();
		selectStatement.close();
		
		targetConnection.commit();
		
		targetConnection.setAutoCommit(true);

		EasyMock.expect(context.getTargetDatabase()).andReturn(targetDatabase);
		EasyMock.expect(context.getConnection(targetDatabase)).andReturn(targetConnection);
		
		EasyMock.expect(context.getDatabaseMetaDataCache()).andReturn(metaData);
		EasyMock.expect(metaData.getDatabaseProperties(targetConnection)).andReturn(database);
		EasyMock.expect(database.getTables()).andReturn(Collections.singleton(table));
		EasyMock.expect(context.getDialect()).andReturn(dialect);

		EasyMock.expect(targetConnection.createStatement()).andReturn(targetStatement);

		EasyMock.expect(table.getForeignKeyConstraints()).andReturn(Collections.singleton(foreignKey));
		EasyMock.expect(dialect.getCreateForeignKeyConstraintSQL(foreignKey)).andReturn("create fk");
		
		targetStatement.addBatch("create fk");
		
		EasyMock.expect(targetStatement.executeBatch()).andReturn(null);
		
		targetStatement.close();

		EasyMock.expect(dialect.supportsSequences()).andReturn(true);
		
		EasyMock.expect(context.getSourceDatabase()).andReturn(sourceDatabase);
		EasyMock.expect(context.getConnection(sourceDatabase)).andReturn(sourceConnection);

		EasyMock.expect(context.getDialect()).andReturn(dialect);
		
		Collection<String> sequenceList = Arrays.asList(new String[] { "sequence1", "sequence2" });
		
		EasyMock.expect(dialect.getSequences(sourceConnection)).andReturn(sequenceList);
		EasyMock.expect(context.getActiveDatabaseSet()).andReturn(Collections.singleton(sourceDatabase));
		EasyMock.expect(context.getExecutor()).andReturn(executor);
		
		EasyMock.expect(dialect.getNextSequenceValueSQL("sequence1")).andReturn("sequence1 next value");

		EasyMock.expect(context.getConnection(sourceDatabase)).andReturn(sourceConnection);
		EasyMock.expect(sourceConnection.createStatement()).andReturn(sourceStatement);
		EasyMock.expect(sourceStatement.executeQuery("sequence1 next value")).andReturn(sourceResultSet);
		
		EasyMock.expect(sourceResultSet.next()).andReturn(true);
		
		EasyMock.expect(sourceResultSet.getLong(1)).andReturn(1L);
		
		sourceResultSet.close();
		sourceStatement.close();

		EasyMock.expect(dialect.getNextSequenceValueSQL("sequence2")).andReturn("sequence2 next value");
		
		EasyMock.expect(context.getConnection(sourceDatabase)).andReturn(sourceConnection);
		EasyMock.expect(sourceConnection.createStatement()).andReturn(sourceStatement);
		EasyMock.expect(sourceStatement.executeQuery("sequence2 next value")).andReturn(sourceResultSet);
		
		EasyMock.expect(sourceResultSet.next()).andReturn(true);
		
		EasyMock.expect(sourceResultSet.getLong(1)).andReturn(2L);
		
		sourceResultSet.close();
		sourceStatement.close();

		EasyMock.expect(context.getTargetDatabase()).andReturn(targetDatabase);
		EasyMock.expect(context.getConnection(targetDatabase)).andReturn(targetConnection);
		EasyMock.expect(targetConnection.createStatement()).andReturn(targetStatement);
		
		EasyMock.expect(dialect.getAlterSequenceSQL("sequence1", 2L)).andReturn("alter sequence1");
		
		targetStatement.addBatch("alter sequence1");

		EasyMock.expect(dialect.getAlterSequenceSQL("sequence2", 3L)).andReturn("alter sequence2");
		
		targetStatement.addBatch("alter sequence2");
		
		EasyMock.expect(targetStatement.executeBatch()).andReturn(null);
		
		targetStatement.close();
		
		EasyMock.replay(context, sourceDatabase, targetDatabase, sourceConnection, targetConnection, statement, metaData, database, table, dialect, targetStatement, sourceStatement, sourceResultSet, foreignKey, selectStatement, resultSet, deleteStatement, insertStatement, column1, column2);

		this.strategy.synchronize(context);
		
		EasyMock.verify(context, sourceDatabase, targetDatabase, sourceConnection, targetConnection, statement, metaData, database, table, dialect, targetStatement, sourceStatement, sourceResultSet, foreignKey, selectStatement, resultSet, deleteStatement, insertStatement, column1, column2);
	}
}
