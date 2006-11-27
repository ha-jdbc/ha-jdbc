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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.SynchronizationContext;
import net.sf.hajdbc.SynchronizationStrategy;
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
	@Test(dataProvider = "context")
	public void synchronize(SynchronizationContext context) throws SQLException
	{
		ForeignKeyConstraint foreignKey = this.control.createMock(ForeignKeyConstraint.class);
		UniqueConstraint primaryKey = this.control.createMock(UniqueConstraint.class);
		UniqueConstraint uniqueKey = this.control.createMock(UniqueConstraint.class);
		Statement targetStatement = this.control.createMock(Statement.class);
		ResultSet targetResultSet = this.control.createMock(ResultSet.class);
		Statement sourceStatement = this.control.createMock(Statement.class);
		ResultSet sourceResultSet = this.control.createMock(ResultSet.class);
		PreparedStatement deleteStatement = this.control.createMock(PreparedStatement.class);
		PreparedStatement insertStatement = this.control.createMock(PreparedStatement.class);
		PreparedStatement updateStatement = this.control.createMock(PreparedStatement.class);
		ColumnProperties column1 = this.control.createMock(ColumnProperties.class);
		ColumnProperties column2 = this.control.createMock(ColumnProperties.class);
		ColumnProperties column3 = this.control.createMock(ColumnProperties.class);
		ColumnProperties column4 = this.control.createMock(ColumnProperties.class);
		
		EasyMock.expect(context.getSourceDatabase()).andReturn(this.sourceDatabase);
		EasyMock.expect(context.getConnection(this.sourceDatabase)).andReturn(this.sourceConnection);

		EasyMock.expect(context.getTargetDatabase()).andReturn(this.targetDatabase);
		EasyMock.expect(context.getConnection(this.targetDatabase)).andReturn(this.targetConnection);
		
		EasyMock.expect(context.getDialect()).andReturn(this.dialect);
		
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

		EasyMock.expect(context.getDatabaseMetaDataCache()).andReturn(this.metaData);
		EasyMock.expect(this.metaData.getDatabaseProperties(this.targetConnection)).andReturn(this.database);
		EasyMock.expect(this.database.getTables()).andReturn(Collections.singleton(this.table));
		
		EasyMock.expect(this.table.getUniqueConstraints()).andReturn(new ArrayList<UniqueConstraint>(Arrays.asList(new UniqueConstraint[] { primaryKey, uniqueKey })));
		EasyMock.expect(this.table.getPrimaryKey()).andReturn(primaryKey);
		EasyMock.expect(context.getDialect()).andReturn(this.dialect);
		
		EasyMock.expect(context.getTargetDatabase()).andReturn(this.targetDatabase);
		EasyMock.expect(context.getConnection(this.targetDatabase)).andReturn(this.targetConnection);
		EasyMock.expect(this.targetConnection.createStatement()).andReturn(targetStatement);
		
		EasyMock.expect(this.dialect.getDropUniqueConstraintSQL(uniqueKey)).andReturn("drop uk");
		
		targetStatement.addBatch("drop uk");

		EasyMock.expect(targetStatement.executeBatch()).andReturn(null);

		targetStatement.close();

		this.targetConnection.setAutoCommit(false);

		EasyMock.expect(this.table.getName()).andReturn("table");
		EasyMock.expect(this.table.getPrimaryKey()).andReturn(primaryKey);
		EasyMock.expect(primaryKey.getColumnList()).andReturn(Arrays.asList(new String[] { "column1", "column2" }));
		
		EasyMock.expect(this.table.getColumns()).andReturn(Arrays.asList(new String[] { "column1", "column2", "column3", "column4" }));
		
		EasyMock.expect(this.targetConnection.createStatement()).andReturn(targetStatement);
		targetStatement.setFetchSize(0);

		// Disable order checking, since statement is executed asynchronously
		this.control.checkOrder(false);
		
		EasyMock.expect(targetStatement.executeQuery("SELECT column1, column2, column3, column4 FROM table ORDER BY column1, column2")).andReturn(targetResultSet);

		EasyMock.expect(this.sourceConnection.createStatement()).andReturn(sourceStatement);
		
		sourceStatement.setFetchSize(0);
		
		EasyMock.expect(sourceStatement.executeQuery("SELECT column1, column2, column3, column4 FROM table ORDER BY column1, column2")).andReturn(sourceResultSet);

		this.control.checkOrder(true);
		
		EasyMock.expect(this.targetConnection.prepareStatement("DELETE FROM table WHERE column1 = ? AND column2 = ?")).andReturn(deleteStatement);
		EasyMock.expect(this.targetConnection.prepareStatement("INSERT INTO table (column1, column2, column3, column4) VALUES (?, ?, ?, ?)")).andReturn(insertStatement);
		EasyMock.expect(this.targetConnection.prepareStatement("UPDATE table SET column3 = ?, column4 = ? WHERE column1 = ? AND column2 = ?")).andReturn(updateStatement);

		EasyMock.expect(sourceResultSet.next()).andReturn(true);
		EasyMock.expect(targetResultSet.next()).andReturn(true);
		
		// Trigger insert
		EasyMock.expect(sourceResultSet.getObject(1)).andReturn(1);
		EasyMock.expect(targetResultSet.getObject(1)).andReturn(1);
		EasyMock.expect(sourceResultSet.getObject(2)).andReturn(1);
		EasyMock.expect(targetResultSet.getObject(2)).andReturn(2);
		
		insertStatement.clearParameters();
		
		EasyMock.expect(this.table.getColumnProperties("column1")).andReturn(column1);
		EasyMock.expect(this.dialect.getColumnType(column1)).andReturn(Types.INTEGER);
		EasyMock.expect(sourceResultSet.getObject(1)).andReturn(1);
		EasyMock.expect(sourceResultSet.wasNull()).andReturn(false);
		insertStatement.setObject(1, 1, Types.INTEGER);
		
		EasyMock.expect(this.table.getColumnProperties("column2")).andReturn(column2);
		EasyMock.expect(this.dialect.getColumnType(column2)).andReturn(Types.INTEGER);
		EasyMock.expect(sourceResultSet.getObject(2)).andReturn(1);
		EasyMock.expect(sourceResultSet.wasNull()).andReturn(false);
		insertStatement.setObject(2, 1, Types.INTEGER);

		EasyMock.expect(this.table.getColumnProperties("column3")).andReturn(column3);
		EasyMock.expect(this.dialect.getColumnType(column3)).andReturn(Types.BLOB);
		EasyMock.expect(sourceResultSet.getBlob(3)).andReturn(null);
		EasyMock.expect(sourceResultSet.wasNull()).andReturn(true);
		insertStatement.setNull(3, Types.BLOB);
		
		EasyMock.expect(this.table.getColumnProperties("column4")).andReturn(column4);
		EasyMock.expect(this.dialect.getColumnType(column4)).andReturn(Types.CLOB);
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
		EasyMock.expect(this.table.getColumnProperties("column3")).andReturn(column3);
		EasyMock.expect(this.dialect.getColumnType(column3)).andReturn(Types.VARCHAR);
		EasyMock.expect(sourceResultSet.getObject(3)).andReturn("");
		EasyMock.expect(targetResultSet.getObject(3)).andReturn("");
		EasyMock.expect(sourceResultSet.wasNull()).andReturn(false);
		updateStatement.setObject(1, "", Types.VARCHAR);
		EasyMock.expect(targetResultSet.wasNull()).andReturn(false);
		
		// Nothing to update
		EasyMock.expect(this.table.getColumnProperties("column4")).andReturn(column4);
		EasyMock.expect(this.dialect.getColumnType(column4)).andReturn(Types.VARCHAR);
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

		EasyMock.expect(this.table.getColumnProperties("column3")).andReturn(column3);
		EasyMock.expect(this.dialect.getColumnType(column3)).andReturn(Types.VARCHAR);
		EasyMock.expect(sourceResultSet.getObject(3)).andReturn("");
		EasyMock.expect(targetResultSet.getObject(3)).andReturn(null);
		EasyMock.expect(sourceResultSet.wasNull()).andReturn(false);
		updateStatement.setObject(1, "", Types.VARCHAR);
		EasyMock.expect(targetResultSet.wasNull()).andReturn(true);
		
		EasyMock.expect(this.table.getColumnProperties("column4")).andReturn(column4);
		EasyMock.expect(this.dialect.getColumnType(column4)).andReturn(Types.VARCHAR);
		EasyMock.expect(sourceResultSet.getObject(4)).andReturn(null);
		EasyMock.expect(targetResultSet.getObject(4)).andReturn("");
		EasyMock.expect(sourceResultSet.wasNull()).andReturn(true);
		updateStatement.setNull(2, Types.VARCHAR);
		EasyMock.expect(targetResultSet.wasNull()).andReturn(false);
		
		EasyMock.expect(this.table.getColumnProperties("column1")).andReturn(column1);
		EasyMock.expect(this.dialect.getColumnType(column1)).andReturn(Types.INTEGER);
		EasyMock.expect(targetResultSet.getObject(1)).andReturn(1);
		updateStatement.setObject(3, 1, Types.INTEGER);
		
		EasyMock.expect(this.table.getColumnProperties("column2")).andReturn(column2);
		EasyMock.expect(this.dialect.getColumnType(column2)).andReturn(Types.INTEGER);
		EasyMock.expect(targetResultSet.getObject(2)).andReturn(3);
		updateStatement.setObject(4, 3, Types.INTEGER);

		updateStatement.addBatch();
		
		EasyMock.expect(sourceResultSet.next()).andReturn(false);
		EasyMock.expect(targetResultSet.next()).andReturn(true);
		
		deleteStatement.clearParameters();

		EasyMock.expect(this.table.getColumnProperties("column1")).andReturn(column1);
		EasyMock.expect(this.dialect.getColumnType(column1)).andReturn(Types.INTEGER);
		EasyMock.expect(targetResultSet.getObject(1)).andReturn(2);
		deleteStatement.setObject(1, 2, Types.INTEGER);
		
		EasyMock.expect(this.table.getColumnProperties("column2")).andReturn(column2);
		EasyMock.expect(this.dialect.getColumnType(column2)).andReturn(Types.INTEGER);
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

		this.targetConnection.commit();
		
		this.targetConnection.setAutoCommit(true);

		EasyMock.expect(this.table.getUniqueConstraints()).andReturn(new ArrayList<UniqueConstraint>(Arrays.asList(new UniqueConstraint[] { primaryKey, uniqueKey })));
		EasyMock.expect(this.table.getPrimaryKey()).andReturn(primaryKey);
		EasyMock.expect(context.getDialect()).andReturn(this.dialect);
		
		EasyMock.expect(context.getTargetDatabase()).andReturn(this.targetDatabase);
		EasyMock.expect(context.getConnection(this.targetDatabase)).andReturn(this.targetConnection);
		EasyMock.expect(this.targetConnection.createStatement()).andReturn(targetStatement);
		
		EasyMock.expect(this.dialect.getCreateUniqueConstraintSQL(uniqueKey)).andReturn("create uk");
		
		targetStatement.addBatch("create uk");
		EasyMock.expect(targetStatement.executeBatch()).andReturn(null);

		targetStatement.close();
		
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
	}
}
