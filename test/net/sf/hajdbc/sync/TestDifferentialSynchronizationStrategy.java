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
import java.util.ArrayList;
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
import net.sf.hajdbc.UniqueConstraint;

import org.easymock.EasyMock;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
public class TestDifferentialSynchronizationStrategy extends TestLockingSynchronizationStrategy
{
	/**
	 * @see net.sf.hajdbc.sync.TestLockingSynchronizationStrategy#createSynchronizationStrategy()
	 */
	@Override
	protected SynchronizationStrategy createSynchronizationStrategy()
	{
		return new DifferentialSynchronizationStrategy();
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
		ForeignKeyConstraint foreignKey = EasyMock.createStrictMock(ForeignKeyConstraint.class);
		UniqueConstraint primaryKey = EasyMock.createStrictMock(UniqueConstraint.class);
		UniqueConstraint uniqueKey = EasyMock.createStrictMock(UniqueConstraint.class);
		Statement targetStatement = EasyMock.createStrictMock(Statement.class);
		ResultSet targetResultSet = EasyMock.createStrictMock(ResultSet.class);
		Statement sourceStatement = EasyMock.createStrictMock(Statement.class);
		ResultSet sourceResultSet = EasyMock.createStrictMock(ResultSet.class);
		PreparedStatement deleteStatement = EasyMock.createStrictMock(PreparedStatement.class);
		PreparedStatement insertStatement = EasyMock.createStrictMock(PreparedStatement.class);
		PreparedStatement updateStatement = EasyMock.createStrictMock(PreparedStatement.class);
		ColumnProperties column1 = EasyMock.createStrictMock(ColumnProperties.class);
		ColumnProperties column2 = EasyMock.createStrictMock(ColumnProperties.class);
		ColumnProperties column3 = EasyMock.createStrictMock(ColumnProperties.class);
		ColumnProperties column4 = EasyMock.createStrictMock(ColumnProperties.class);
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

		EasyMock.expect(context.getDatabaseMetaDataCache()).andReturn(metaData);
		EasyMock.expect(metaData.getDatabaseProperties(targetConnection)).andReturn(database);
		EasyMock.expect(database.getTables()).andReturn(Collections.singleton(table));
		
		EasyMock.expect(table.getUniqueConstraints()).andReturn(new ArrayList<UniqueConstraint>(Arrays.asList(new UniqueConstraint[] { primaryKey, uniqueKey })));
		EasyMock.expect(table.getPrimaryKey()).andReturn(primaryKey);
		EasyMock.expect(context.getDialect()).andReturn(dialect);
		
		EasyMock.expect(context.getTargetDatabase()).andReturn(targetDatabase);
		EasyMock.expect(context.getConnection(targetDatabase)).andReturn(targetConnection);
		EasyMock.expect(targetConnection.createStatement()).andReturn(targetStatement);
		
		EasyMock.expect(dialect.getDropUniqueConstraintSQL(uniqueKey)).andReturn("drop uk");
		
		targetStatement.addBatch("drop uk");

		EasyMock.expect(targetStatement.executeBatch()).andReturn(null);

		targetStatement.close();

		targetConnection.setAutoCommit(false);

		EasyMock.expect(table.getName()).andReturn("table");
		EasyMock.expect(table.getPrimaryKey()).andReturn(primaryKey);
		EasyMock.expect(primaryKey.getColumnList()).andReturn(Arrays.asList(new String[] { "column1", "column2" }));
		
		EasyMock.expect(table.getColumns()).andReturn(Arrays.asList(new String[] { "column1", "column2", "column3", "column4" }));
		
		EasyMock.expect(targetConnection.createStatement()).andReturn(targetStatement);
		targetStatement.setFetchSize(0);

		// Disable order checking, since statement is executed asynchronously
		EasyMock.checkOrder(targetStatement, false);
		EasyMock.checkOrder(sourceConnection, false);
		EasyMock.checkOrder(sourceStatement, false);
		
		EasyMock.expect(targetStatement.executeQuery("SELECT column1, column2, column3, column4 FROM table ORDER BY column1, column2")).andReturn(targetResultSet);

		EasyMock.expect(sourceConnection.createStatement()).andReturn(sourceStatement);
		
		sourceStatement.setFetchSize(0);
		
		EasyMock.expect(sourceStatement.executeQuery("SELECT column1, column2, column3, column4 FROM table ORDER BY column1, column2")).andReturn(sourceResultSet);

		EasyMock.checkOrder(targetStatement, true);
		EasyMock.checkOrder(sourceConnection, true);
		EasyMock.checkOrder(sourceStatement, true);
		
		EasyMock.expect(targetConnection.prepareStatement("DELETE FROM table WHERE column1 = ? AND column2 = ?")).andReturn(deleteStatement);
		EasyMock.expect(targetConnection.prepareStatement("INSERT INTO table (column1, column2, column3, column4) VALUES (?, ?, ?, ?)")).andReturn(insertStatement);
		EasyMock.expect(targetConnection.prepareStatement("UPDATE table SET column3 = ?, column4 = ? WHERE column1 = ? AND column2 = ?")).andReturn(updateStatement);

		EasyMock.expect(sourceResultSet.next()).andReturn(true);
		EasyMock.expect(targetResultSet.next()).andReturn(true);
		
		// Trigger insert
		EasyMock.expect(sourceResultSet.getObject(1)).andReturn(1);
		EasyMock.expect(targetResultSet.getObject(1)).andReturn(1);
		EasyMock.expect(sourceResultSet.getObject(2)).andReturn(1);
		EasyMock.expect(targetResultSet.getObject(2)).andReturn(2);
		
		insertStatement.clearParameters();
		
		EasyMock.expect(table.getColumnProperties("column1")).andReturn(column1);
		EasyMock.expect(dialect.getColumnType(column1)).andReturn(Types.INTEGER);
		EasyMock.expect(sourceResultSet.getObject(1)).andReturn(1);
		EasyMock.expect(sourceResultSet.wasNull()).andReturn(false);
		insertStatement.setObject(1, 1, Types.INTEGER);
		
		EasyMock.expect(table.getColumnProperties("column2")).andReturn(column2);
		EasyMock.expect(dialect.getColumnType(column2)).andReturn(Types.INTEGER);
		EasyMock.expect(sourceResultSet.getObject(2)).andReturn(1);
		EasyMock.expect(sourceResultSet.wasNull()).andReturn(false);
		insertStatement.setObject(2, 1, Types.INTEGER);

		EasyMock.expect(table.getColumnProperties("column3")).andReturn(column3);
		EasyMock.expect(dialect.getColumnType(column3)).andReturn(Types.BLOB);
		EasyMock.expect(sourceResultSet.getBlob(3)).andReturn(null);
		EasyMock.expect(sourceResultSet.wasNull()).andReturn(true);
		insertStatement.setNull(3, Types.BLOB);
		
		EasyMock.expect(table.getColumnProperties("column4")).andReturn(column4);
		EasyMock.expect(dialect.getColumnType(column4)).andReturn(Types.CLOB);
		EasyMock.expect(sourceResultSet.getClob(4)).andReturn(null);
		EasyMock.expect(sourceResultSet.wasNull()).andReturn(true);
		insertStatement.setNull(4, Types.CLOB);
		
		insertStatement.addBatch();
		
		EasyMock.expect(sourceResultSet.next()).andReturn(true);
		
		// Trigger update
		EasyMock.expect(sourceResultSet.getObject(1)).andReturn(1);
		EasyMock.expect(targetResultSet.getObject(1)).andReturn(1);
		EasyMock.expect(sourceResultSet.getObject(2)).andReturn(2);
		EasyMock.expect(targetResultSet.getObject(2)).andReturn(2);

		updateStatement.clearParameters();
		
		// Nothing to update
		EasyMock.expect(table.getColumnProperties("column3")).andReturn(column3);
		EasyMock.expect(dialect.getColumnType(column3)).andReturn(Types.VARCHAR);
		EasyMock.expect(sourceResultSet.getObject(3)).andReturn("");
		EasyMock.expect(targetResultSet.getObject(3)).andReturn("");
		EasyMock.expect(sourceResultSet.wasNull()).andReturn(false);
		updateStatement.setObject(1, "", Types.VARCHAR);
		EasyMock.expect(targetResultSet.wasNull()).andReturn(false);
		
		// Nothing to update
		EasyMock.expect(table.getColumnProperties("column4")).andReturn(column4);
		EasyMock.expect(dialect.getColumnType(column4)).andReturn(Types.VARCHAR);
		EasyMock.expect(sourceResultSet.getObject(4)).andReturn(null);
		EasyMock.expect(targetResultSet.getObject(4)).andReturn(null);
		EasyMock.expect(sourceResultSet.wasNull()).andReturn(true);
		updateStatement.setNull(2, Types.VARCHAR);
		EasyMock.expect(targetResultSet.wasNull()).andReturn(true);
		
		EasyMock.expect(sourceResultSet.next()).andReturn(true);
		EasyMock.expect(targetResultSet.next()).andReturn(true);
		
		// Trigger update
		EasyMock.expect(sourceResultSet.getObject(1)).andReturn(1);
		EasyMock.expect(targetResultSet.getObject(1)).andReturn(1);
		EasyMock.expect(sourceResultSet.getObject(2)).andReturn(3);
		EasyMock.expect(targetResultSet.getObject(2)).andReturn(3);

		updateStatement.clearParameters();

		EasyMock.expect(table.getColumnProperties("column3")).andReturn(column3);
		EasyMock.expect(dialect.getColumnType(column3)).andReturn(Types.VARCHAR);
		EasyMock.expect(sourceResultSet.getObject(3)).andReturn("");
		EasyMock.expect(targetResultSet.getObject(3)).andReturn(null);
		EasyMock.expect(sourceResultSet.wasNull()).andReturn(false);
		updateStatement.setObject(1, "", Types.VARCHAR);
		EasyMock.expect(targetResultSet.wasNull()).andReturn(true);
		
		EasyMock.expect(table.getColumnProperties("column4")).andReturn(column4);
		EasyMock.expect(dialect.getColumnType(column4)).andReturn(Types.VARCHAR);
		EasyMock.expect(sourceResultSet.getObject(4)).andReturn(null);
		EasyMock.expect(targetResultSet.getObject(4)).andReturn("");
		EasyMock.expect(sourceResultSet.wasNull()).andReturn(true);
		updateStatement.setNull(2, Types.VARCHAR);
		EasyMock.expect(targetResultSet.wasNull()).andReturn(false);
		
		EasyMock.expect(table.getColumnProperties("column1")).andReturn(column1);
		EasyMock.expect(dialect.getColumnType(column1)).andReturn(Types.INTEGER);
		EasyMock.expect(targetResultSet.getObject(1)).andReturn(1);
		updateStatement.setObject(3, 1, Types.INTEGER);
		
		EasyMock.expect(table.getColumnProperties("column2")).andReturn(column2);
		EasyMock.expect(dialect.getColumnType(column2)).andReturn(Types.INTEGER);
		EasyMock.expect(targetResultSet.getObject(2)).andReturn(3);
		updateStatement.setObject(4, 3, Types.INTEGER);

		updateStatement.addBatch();
		
		EasyMock.expect(sourceResultSet.next()).andReturn(false);
		EasyMock.expect(targetResultSet.next()).andReturn(true);
		
		deleteStatement.clearParameters();

		EasyMock.expect(table.getColumnProperties("column1")).andReturn(column1);
		EasyMock.expect(dialect.getColumnType(column1)).andReturn(Types.INTEGER);
		EasyMock.expect(targetResultSet.getObject(1)).andReturn(2);
		deleteStatement.setObject(1, 2, Types.INTEGER);
		
		EasyMock.expect(table.getColumnProperties("column2")).andReturn(column2);
		EasyMock.expect(dialect.getColumnType(column2)).andReturn(Types.INTEGER);
		EasyMock.expect(targetResultSet.getObject(2)).andReturn(1);
		deleteStatement.setObject(2, 1, Types.INTEGER);
		
		deleteStatement.addBatch();
		
		EasyMock.expect(targetResultSet.next()).andReturn(false);
		
		EasyMock.expect(deleteStatement.executeBatch()).andReturn(null);
		deleteStatement.close();
		
		EasyMock.expect(insertStatement.executeBatch()).andReturn(null);
		insertStatement.close();
		
		EasyMock.expect(updateStatement.executeBatch()).andReturn(null);
		updateStatement.close();
		
		targetStatement.close();
		sourceStatement.close();

		targetConnection.commit();
		
		targetConnection.setAutoCommit(true);

		EasyMock.expect(table.getUniqueConstraints()).andReturn(new ArrayList<UniqueConstraint>(Arrays.asList(new UniqueConstraint[] { primaryKey, uniqueKey })));
		EasyMock.expect(table.getPrimaryKey()).andReturn(primaryKey);
		EasyMock.expect(context.getDialect()).andReturn(dialect);
		
		EasyMock.expect(context.getTargetDatabase()).andReturn(targetDatabase);
		EasyMock.expect(context.getConnection(targetDatabase)).andReturn(targetConnection);
		EasyMock.expect(targetConnection.createStatement()).andReturn(targetStatement);
		
		EasyMock.expect(dialect.getCreateUniqueConstraintSQL(uniqueKey)).andReturn("create uk");
		
		targetStatement.addBatch("create uk");
		EasyMock.expect(targetStatement.executeBatch()).andReturn(null);

		targetStatement.close();
		
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

		EasyMock.replay(context, sourceDatabase, targetDatabase, sourceConnection, targetConnection, statement, metaData, database, table, dialect, foreignKey, primaryKey, uniqueKey, targetStatement, targetResultSet, sourceStatement, sourceResultSet, deleteStatement, insertStatement, updateStatement, column1, column2, column3, column4);
		
		this.strategy.synchronize(context);
		
		EasyMock.verify(context, sourceDatabase, targetDatabase, sourceConnection, targetConnection, statement, metaData, database, table, dialect, foreignKey, primaryKey, uniqueKey, targetStatement, targetResultSet, sourceStatement, sourceResultSet, deleteStatement, insertStatement, updateStatement, column1, column2, column3, column4);
	}
}
