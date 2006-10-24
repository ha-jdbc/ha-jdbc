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

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.DatabaseMetaDataCache;
import net.sf.hajdbc.DatabaseProperties;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.TableProperties;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
public class TestFullSynchronizationStrategy implements SynchronizationStrategy
{
	private IMocksControl control = EasyMock.createControl();
	
	private SynchronizationStrategy strategy = new FullSynchronizationStrategy();
	
	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#requiresTableLocking()
	 */
	@Test
	public boolean requiresTableLocking()
	{
		boolean requires = this.strategy.requiresTableLocking();
		
		assert requires;
		
		return requires;
	}

	@DataProvider(name = "sync")
	public Object[][] syncProvider()
	{
		return new Object[][] { new Object[] { this.control.createMock(Connection.class), this.control.createMock(Connection.class), this.control.createMock(DatabaseMetaDataCache.class), this.control.createMock(Dialect.class) } };
	}

	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#synchronize(java.sql.Connection, java.sql.Connection, net.sf.hajdbc.DatabaseMetaDataCache, net.sf.hajdbc.Dialect)
	 */
	@Test(dataProvider = "sync")
	public void synchronize(Connection inactiveConnection, Connection activeConnection, DatabaseMetaDataCache metaData, Dialect dialect) throws SQLException
	{
		Statement statement = this.control.createMock(Statement.class);
		DatabaseProperties database = this.control.createMock(DatabaseProperties.class);
		TableProperties table = this.control.createMock(TableProperties.class);
		ForeignKeyConstraint foreignKey = this.control.createMock(ForeignKeyConstraint.class);
		Statement selectStatement = this.control.createMock(Statement.class);
		ResultSet resultSet = this.control.createMock(ResultSet.class);
		Statement deleteStatement = this.control.createMock(Statement.class);
		PreparedStatement insertStatement = this.control.createMock(PreparedStatement.class);
		ColumnProperties column1 = this.control.createMock(ColumnProperties.class);
		ColumnProperties column2 = this.control.createMock(ColumnProperties.class);
		Statement activeStatement = this.control.createMock(Statement.class);
		
		inactiveConnection.setAutoCommit(true);
		
		EasyMock.expect(inactiveConnection.createStatement()).andReturn(statement);
		
		EasyMock.expect(metaData.getDatabaseProperties(inactiveConnection)).andReturn(database);
		EasyMock.expect(database.getTables()).andReturn(Collections.singleton(table));
		EasyMock.expect(table.getForeignKeyConstraints()).andReturn(Collections.singleton(foreignKey));
		EasyMock.expect(dialect.getDropForeignKeyConstraintSQL(foreignKey)).andReturn("drop fk");
		
		statement.addBatch("drop fk");
		EasyMock.expect(statement.executeBatch()).andReturn(null);
		statement.clearBatch();
		
		inactiveConnection.setAutoCommit(false);
		
		EasyMock.expect(table.getName()).andReturn("table");
		EasyMock.expect(table.getColumns()).andReturn(Arrays.asList(new String[] { "column1", "column2" }));
		
		EasyMock.expect(activeConnection.createStatement()).andReturn(selectStatement);
		selectStatement.setFetchSize(0);
		
		this.control.checkOrder(false);
		
		EasyMock.expect(dialect.getTruncateTableSQL(table)).andReturn("DELETE FROM table");
		EasyMock.expect(inactiveConnection.createStatement()).andReturn(deleteStatement);
		EasyMock.expect(deleteStatement.executeUpdate("DELETE FROM table")).andReturn(0);
		
		deleteStatement.close();
		
		EasyMock.expect(selectStatement.executeQuery("SELECT column1, column2 FROM table")).andReturn(resultSet);
		
		this.control.checkOrder(true);
		
		EasyMock.expect(inactiveConnection.prepareStatement("INSERT INTO table (column1, column2) VALUES (?, ?)")).andReturn(insertStatement);
		
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
		
		inactiveConnection.commit();
		
		inactiveConnection.setAutoCommit(true);

		EasyMock.expect(table.getForeignKeyConstraints()).andReturn(Collections.singleton(foreignKey));
		EasyMock.expect(dialect.getCreateForeignKeyConstraintSQL(foreignKey)).andReturn("create fk");
		
		statement.addBatch("create fk");
		EasyMock.expect(statement.executeBatch()).andReturn(null);
		statement.clearBatch();
		
		EasyMock.expect(dialect.supportsSequences()).andReturn(true);
		
		Collection<String> sequenceList = Arrays.asList(new String[] { "sequence1", "sequence2" });
		
		EasyMock.expect(dialect.getSequences(activeConnection)).andReturn(sequenceList);

		EasyMock.expect(activeConnection.createStatement()).andReturn(activeStatement);

		EasyMock.expect(dialect.getCurrentSequenceValueSQL("sequence1")).andReturn("sequence1 current value");
		
		EasyMock.expect(activeStatement.executeQuery("sequence1 current value")).andReturn(resultSet);
		EasyMock.expect(resultSet.next()).andReturn(true);
		EasyMock.expect(resultSet.getLong(1)).andReturn(1L);
		
		resultSet.close();

		EasyMock.expect(dialect.getAlterSequenceSQL("sequence1", 1L)).andReturn("alter sequence1");
		
		statement.addBatch("alter sequence1");

		EasyMock.expect(dialect.getCurrentSequenceValueSQL("sequence2")).andReturn("sequence2 current value");
		
		EasyMock.expect(activeStatement.executeQuery("sequence2 current value")).andReturn(resultSet);
		EasyMock.expect(resultSet.next()).andReturn(true);
		EasyMock.expect(resultSet.getLong(1)).andReturn(2L);
		
		resultSet.close();

		EasyMock.expect(dialect.getAlterSequenceSQL("sequence2", 2L)).andReturn("alter sequence2");
		
		statement.addBatch("alter sequence2");
		
		activeStatement.close();
		
		EasyMock.expect(statement.executeBatch()).andReturn(null);

		statement.close();
		
		this.control.replay();
		
		this.strategy.synchronize(inactiveConnection, activeConnection, metaData, dialect);
		
		this.control.verify();
		this.control.reset();
	}
}
