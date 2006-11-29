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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.SynchronizationContext;
import net.sf.hajdbc.SynchronizationStrategy;

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
	@Test(dataProvider = "context")
	public void synchronize(SynchronizationContext context) throws SQLException
	{
		Statement targetStatement = this.control.createMock(Statement.class);
		Statement sourceStatement = this.control.createMock(Statement.class);
		ResultSet sourceResultSet = this.control.createMock(ResultSet.class);
		ForeignKeyConstraint foreignKey = this.control.createMock(ForeignKeyConstraint.class);
		Statement selectStatement = this.control.createMock(Statement.class);
		ResultSet resultSet = this.control.createMock(ResultSet.class);
		Statement deleteStatement = this.control.createMock(Statement.class);
		PreparedStatement insertStatement = this.control.createMock(PreparedStatement.class);
		ColumnProperties column1 = this.control.createMock(ColumnProperties.class);
		ColumnProperties column2 = this.control.createMock(ColumnProperties.class);
		
		EasyMock.expect(context.getSourceDatabase()).andReturn(this.sourceDatabase);
		EasyMock.expect(context.getConnection(this.sourceDatabase)).andReturn(this.sourceConnection);

		EasyMock.expect(context.getTargetDatabase()).andReturn(this.targetDatabase);
		EasyMock.expect(context.getConnection(this.targetDatabase)).andReturn(this.targetConnection);
		
		EasyMock.expect(context.getDialect()).andReturn(this.dialect);
		EasyMock.expect(context.getExecutor()).andReturn(this.executor);

		this.targetConnection.setAutoCommit(true);
		
		EasyMock.expect(context.getTargetDatabase()).andReturn(this.targetDatabase);
		EasyMock.expect(context.getConnection(this.targetDatabase)).andReturn(this.targetConnection);
		
		EasyMock.expect(context.getDatabaseMetaDataCache()).andReturn(this.metaData);
		EasyMock.expect(this.metaData.getDatabaseProperties(this.targetConnection)).andReturn(this.database);
		EasyMock.expect(this.database.getTables()).andReturn(Collections.singleton(this.table));
		EasyMock.expect(context.getDialect()).andReturn(this.dialect);

		EasyMock.expect(this.targetConnection.createStatement()).andReturn(targetStatement);
		
		EasyMock.expect(this.table.getForeignKeyConstraints()).andReturn(Collections.singleton(foreignKey));
		EasyMock.expect(this.dialect.getDropForeignKeyConstraintSQL(foreignKey)).andReturn("drop fk");
		
		targetStatement.addBatch("drop fk");

		EasyMock.expect(targetStatement.executeBatch()).andReturn(null);

		targetStatement.close();
		
		this.targetConnection.setAutoCommit(false);

		EasyMock.expect(context.getDatabaseMetaDataCache()).andReturn(this.metaData);
		EasyMock.expect(this.metaData.getDatabaseProperties(this.sourceConnection)).andReturn(this.database);
		EasyMock.expect(this.database.getTables()).andReturn(Collections.singleton(this.table));
		
		EasyMock.expect(this.table.getName()).andReturn("table");
		EasyMock.expect(this.table.getColumns()).andReturn(Arrays.asList(new String[] { "column1", "column2" }));
		
		EasyMock.expect(this.sourceConnection.createStatement()).andReturn(selectStatement);
		selectStatement.setFetchSize(0);
		
		this.control.checkOrder(false);
		
		EasyMock.expect(this.dialect.getTruncateTableSQL(this.table)).andReturn("DELETE FROM table");
		EasyMock.expect(this.targetConnection.createStatement()).andReturn(deleteStatement);
		EasyMock.expect(deleteStatement.executeUpdate("DELETE FROM table")).andReturn(0);
		
		deleteStatement.close();
		
		EasyMock.expect(selectStatement.executeQuery("SELECT column1, column2 FROM table")).andReturn(resultSet);
		
		this.control.checkOrder(true);
		
		EasyMock.expect(this.targetConnection.prepareStatement("INSERT INTO table (column1, column2) VALUES (?, ?)")).andReturn(insertStatement);
		
		EasyMock.expect(resultSet.next()).andReturn(true);
		
		EasyMock.expect(this.table.getColumnProperties("column1")).andReturn(column1);
		EasyMock.expect(this.dialect.getColumnType(column1)).andReturn(Types.INTEGER);
		EasyMock.expect(resultSet.getObject(1)).andReturn(1);
		EasyMock.expect(resultSet.wasNull()).andReturn(false);
		insertStatement.setObject(1, 1, Types.INTEGER);
		
		EasyMock.expect(this.table.getColumnProperties("column2")).andReturn(column2);
		EasyMock.expect(this.dialect.getColumnType(column2)).andReturn(Types.VARCHAR);
		EasyMock.expect(resultSet.getObject(2)).andReturn("");
		EasyMock.expect(resultSet.wasNull()).andReturn(false);
		insertStatement.setObject(2, "", Types.VARCHAR);
		
		insertStatement.addBatch();
		insertStatement.clearParameters();
		
		EasyMock.expect(resultSet.next()).andReturn(true);
		
		EasyMock.expect(this.table.getColumnProperties("column1")).andReturn(column1);
		EasyMock.expect(this.dialect.getColumnType(column1)).andReturn(Types.BLOB);
		EasyMock.expect(resultSet.getBlob(1)).andReturn(null);
		EasyMock.expect(resultSet.wasNull()).andReturn(true);
		insertStatement.setNull(1, Types.BLOB);
		
		EasyMock.expect(this.table.getColumnProperties("column2")).andReturn(column2);
		EasyMock.expect(this.dialect.getColumnType(column2)).andReturn(Types.CLOB);
		EasyMock.expect(resultSet.getClob(2)).andReturn(null);
		EasyMock.expect(resultSet.wasNull()).andReturn(true);
		insertStatement.setNull(2, Types.CLOB);
		
		insertStatement.addBatch();
		insertStatement.clearParameters();
		
		EasyMock.expect(resultSet.next()).andReturn(false);
		
		EasyMock.expect(insertStatement.executeBatch()).andReturn(null);
		
		insertStatement.close();
		selectStatement.close();
		
		this.targetConnection.commit();
		
		this.targetConnection.setAutoCommit(true);

		EasyMock.expect(context.getTargetDatabase()).andReturn(this.targetDatabase);
		EasyMock.expect(context.getConnection(this.targetDatabase)).andReturn(this.targetConnection);
		
		EasyMock.expect(context.getDatabaseMetaDataCache()).andReturn(this.metaData);
		EasyMock.expect(this.metaData.getDatabaseProperties(this.targetConnection)).andReturn(this.database);
		EasyMock.expect(this.database.getTables()).andReturn(Collections.singleton(this.table));
		EasyMock.expect(context.getDialect()).andReturn(this.dialect);

		EasyMock.expect(this.targetConnection.createStatement()).andReturn(targetStatement);

		EasyMock.expect(this.table.getForeignKeyConstraints()).andReturn(Collections.singleton(foreignKey));
		EasyMock.expect(this.dialect.getCreateForeignKeyConstraintSQL(foreignKey)).andReturn("create fk");
		
		targetStatement.addBatch("create fk");
		
		EasyMock.expect(targetStatement.executeBatch()).andReturn(null);
		
		targetStatement.close();

		EasyMock.expect(this.dialect.supportsSequences()).andReturn(true);
		
		EasyMock.expect(context.getSourceDatabase()).andReturn(this.sourceDatabase);
		EasyMock.expect(context.getConnection(this.sourceDatabase)).andReturn(this.sourceConnection);

		EasyMock.expect(context.getDialect()).andReturn(this.dialect);
		
		Collection<String> sequenceList = Arrays.asList(new String[] { "sequence1", "sequence2" });
		
		EasyMock.expect(this.dialect.getSequences(this.sourceConnection)).andReturn(sequenceList);
		EasyMock.expect(context.getActiveDatabases()).andReturn(Collections.singleton(this.sourceDatabase));
		EasyMock.expect(context.getExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.dialect.getNextSequenceValueSQL("sequence1")).andReturn("sequence1 next value");

		EasyMock.expect(context.getConnection(this.sourceDatabase)).andReturn(this.sourceConnection);
		EasyMock.expect(this.sourceConnection.createStatement()).andReturn(sourceStatement);
		EasyMock.expect(sourceStatement.executeQuery("sequence1 next value")).andReturn(sourceResultSet);
		
		EasyMock.expect(sourceResultSet.next()).andReturn(true);
		
		EasyMock.expect(sourceResultSet.getLong(1)).andReturn(1L);
		
		sourceResultSet.close();
		sourceStatement.close();

		EasyMock.expect(this.dialect.getNextSequenceValueSQL("sequence2")).andReturn("sequence2 next value");
		
		EasyMock.expect(context.getConnection(this.sourceDatabase)).andReturn(this.sourceConnection);
		EasyMock.expect(this.sourceConnection.createStatement()).andReturn(sourceStatement);
		EasyMock.expect(sourceStatement.executeQuery("sequence2 next value")).andReturn(sourceResultSet);
		
		EasyMock.expect(sourceResultSet.next()).andReturn(true);
		
		EasyMock.expect(sourceResultSet.getLong(1)).andReturn(2L);
		
		sourceResultSet.close();
		sourceStatement.close();

		EasyMock.expect(context.getTargetDatabase()).andReturn(this.targetDatabase);
		EasyMock.expect(context.getConnection(this.targetDatabase)).andReturn(this.targetConnection);
		EasyMock.expect(this.targetConnection.createStatement()).andReturn(targetStatement);
		
		EasyMock.expect(this.dialect.getAlterSequenceSQL("sequence1", 2L)).andReturn("alter sequence1");
		
		targetStatement.addBatch("alter sequence1");

		EasyMock.expect(this.dialect.getAlterSequenceSQL("sequence2", 3L)).andReturn("alter sequence2");
		
		targetStatement.addBatch("alter sequence2");
		
		EasyMock.expect(targetStatement.executeBatch()).andReturn(null);
		
		targetStatement.close();
		
		this.control.replay();
		
		this.strategy.synchronize(context);
		
		this.control.verify();
		this.control.reset();
	}
}
