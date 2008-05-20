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
package net.sf.hajdbc.sync;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseMetaDataCache;
import net.sf.hajdbc.DatabaseProperties;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.SequenceProperties;
import net.sf.hajdbc.SynchronizationContext;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.TableProperties;

import org.easymock.EasyMock;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 */
@SuppressWarnings("nls")
@Test
public class TestFullSynchronizationStrategy extends TestSynchronizationStrategy
{
	public TestFullSynchronizationStrategy()
	{
		super(new FullSynchronizationStrategy());
	}
	
	/**
	 * @see net.sf.hajdbc.sync.TestSynchronizationStrategy#testSynchronize()
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <D> void testSynchronize()
	{
		SynchronizationContext<D> context = EasyMock.createStrictMock(SynchronizationContext.class);
		Database<D> sourceDatabase = EasyMock.createStrictMock(Database.class);
		Database<D> targetDatabase = EasyMock.createStrictMock(Database.class);
		Connection sourceConnection = EasyMock.createStrictMock(Connection.class);
		Connection targetConnection = EasyMock.createStrictMock(Connection.class);
		Statement statement = EasyMock.createStrictMock(Statement.class);
		DatabaseMetaDataCache metaData = EasyMock.createStrictMock(DatabaseMetaDataCache.class);
		DatabaseProperties sourceProperties = EasyMock.createStrictMock(DatabaseProperties.class);
		DatabaseProperties targetProperties = EasyMock.createStrictMock(DatabaseProperties.class);
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
		SequenceProperties sequence = EasyMock.createStrictMock(SequenceProperties.class);
		
		try
		{
			EasyMock.expect(context.getSourceDatabase()).andReturn(sourceDatabase);
			EasyMock.expect(context.getConnection(sourceDatabase)).andReturn(sourceConnection);
	
			EasyMock.expect(context.getTargetDatabase()).andReturn(targetDatabase);
			EasyMock.expect(context.getConnection(targetDatabase)).andReturn(targetConnection);
			
			EasyMock.expect(context.getDialect()).andReturn(dialect);
			EasyMock.expect(context.getExecutor()).andReturn(executor);
	
			targetConnection.setAutoCommit(true);
			{
				EasyMock.expect(context.getDialect()).andReturn(dialect);
		
				EasyMock.expect(context.getTargetDatabase()).andReturn(targetDatabase);
				EasyMock.expect(context.getConnection(targetDatabase)).andReturn(targetConnection);
				
				EasyMock.expect(targetConnection.createStatement()).andReturn(targetStatement);
				
				EasyMock.expect(context.getTargetDatabaseProperties()).andReturn(targetProperties);
				EasyMock.expect(targetProperties.getTables()).andReturn(Collections.singleton(table));
				
				EasyMock.expect(table.getForeignKeyConstraints()).andReturn(Collections.singleton(foreignKey));
				EasyMock.expect(dialect.getDropForeignKeyConstraintSQL(foreignKey)).andReturn("drop fk");
				
				targetStatement.addBatch("drop fk");
		
				EasyMock.expect(targetStatement.executeBatch()).andReturn(null);
		
				targetStatement.close();
			}
			targetConnection.setAutoCommit(false);
	
			EasyMock.expect(context.getSourceDatabaseProperties()).andReturn(sourceProperties);
			EasyMock.expect(sourceProperties.getTables()).andReturn(Collections.singleton(table));
			
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
			{
				EasyMock.expect(context.getDialect()).andReturn(dialect);
		
				EasyMock.expect(context.getTargetDatabase()).andReturn(targetDatabase);
				EasyMock.expect(context.getConnection(targetDatabase)).andReturn(targetConnection);
				
				EasyMock.expect(targetConnection.createStatement()).andReturn(targetStatement);
	
				EasyMock.expect(context.getSourceDatabaseProperties()).andReturn(sourceProperties);
				EasyMock.expect(sourceProperties.getTables()).andReturn(Collections.singleton(table));
				
				EasyMock.expect(table.getForeignKeyConstraints()).andReturn(Collections.singleton(foreignKey));
				EasyMock.expect(dialect.getCreateForeignKeyConstraintSQL(foreignKey)).andReturn("create fk");
				
				targetStatement.addBatch("create fk");
				
				EasyMock.expect(targetStatement.executeBatch()).andReturn(null);
				
				targetStatement.close();
			}
			{
				EasyMock.expect(context.getSourceDatabase()).andReturn(sourceDatabase);
				EasyMock.expect(context.getConnection(sourceDatabase)).andReturn(sourceConnection);
				EasyMock.expect(sourceConnection.createStatement()).andReturn(sourceStatement);
				
				EasyMock.expect(context.getTargetDatabase()).andReturn(targetDatabase);
				EasyMock.expect(context.getConnection(targetDatabase)).andReturn(targetConnection);
				EasyMock.expect(targetConnection.createStatement()).andReturn(targetStatement);
		
				EasyMock.expect(context.getDialect()).andReturn(dialect);
		
				EasyMock.expect(context.getSourceDatabaseProperties()).andReturn(sourceProperties);
				EasyMock.expect(sourceProperties.getTables()).andReturn(Collections.singleton(table));
		
				EasyMock.expect(table.getIdentityColumns()).andReturn(Collections.singleton("column"));
				EasyMock.expect(table.getName()).andReturn("table");
				EasyMock.expect(sourceStatement.executeQuery("SELECT max(column) FROM table")).andReturn(sourceResultSet);
				EasyMock.expect(sourceResultSet.next()).andReturn(true);
				EasyMock.expect(sourceResultSet.getLong(1)).andReturn(1L);
				
				sourceResultSet.close();
				
				EasyMock.expect(table.getColumnProperties("column")).andReturn(column1);
				EasyMock.expect(dialect.getAlterIdentityColumnSQL(table, column1, 2L)).andReturn("column = 1");
				
				targetStatement.addBatch("column = 1");
				EasyMock.expect(targetStatement.executeBatch()).andReturn(null);
				
				sourceStatement.close();
				targetStatement.close();
			}
			{
				EasyMock.expect(context.getSourceDatabaseProperties()).andReturn(sourceProperties);
				EasyMock.expect(sourceProperties.getSequences()).andReturn(Collections.singleton(sequence));
				
				EasyMock.expect(context.getSourceDatabase()).andReturn(sourceDatabase);
				EasyMock.expect(context.getActiveDatabaseSet()).andReturn(Collections.singleton(sourceDatabase));
				EasyMock.expect(context.getExecutor()).andReturn(executor);
				EasyMock.expect(context.getDialect()).andReturn(dialect);
	
				EasyMock.expect(dialect.getNextSequenceValueSQL(sequence)).andReturn("sequence next value");
				
				EasyMock.expect(context.getConnection(sourceDatabase)).andReturn(sourceConnection);
				EasyMock.expect(sourceConnection.createStatement()).andReturn(sourceStatement);
				EasyMock.expect(sourceStatement.executeQuery("sequence next value")).andReturn(sourceResultSet);
				
				EasyMock.expect(sourceResultSet.next()).andReturn(true);
				
				EasyMock.expect(sourceResultSet.getLong(1)).andReturn(1L);
				
				sourceStatement.close();
		
				EasyMock.expect(context.getTargetDatabase()).andReturn(targetDatabase);
				EasyMock.expect(context.getConnection(targetDatabase)).andReturn(targetConnection);
				EasyMock.expect(targetConnection.createStatement()).andReturn(targetStatement);
				
				EasyMock.expect(dialect.getAlterSequenceSQL(sequence, 2L)).andReturn("alter sequence");
				
				targetStatement.addBatch("alter sequence");
				
				EasyMock.expect(targetStatement.executeBatch()).andReturn(null);
				
				targetStatement.close();
			}
			
			EasyMock.replay(context, sourceDatabase, targetDatabase, sourceConnection, targetConnection, statement, metaData, sourceProperties, targetProperties, table, dialect, targetStatement, sourceStatement, sourceResultSet, foreignKey, selectStatement, resultSet, deleteStatement, insertStatement, column1, column2);
	
			this.synchronize(context);
			
			EasyMock.verify(context, sourceDatabase, targetDatabase, sourceConnection, targetConnection, statement, metaData, sourceProperties, targetProperties, table, dialect, targetStatement, sourceStatement, sourceResultSet, foreignKey, selectStatement, resultSet, deleteStatement, insertStatement, column1, column2);
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}
}
