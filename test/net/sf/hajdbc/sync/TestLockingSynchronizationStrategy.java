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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseMetaDataCache;
import net.sf.hajdbc.DatabaseProperties;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.SynchronizationContext;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.TableProperties;

import org.easymock.EasyMock;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
public abstract class TestLockingSynchronizationStrategy implements SynchronizationStrategy
{
	protected SynchronizationStrategy strategy = this.createSynchronizationStrategy();

	protected abstract SynchronizationStrategy createSynchronizationStrategy();
	
	@DataProvider(name = "context")
	Object[][] contextProvider()
	{
		return new Object[][] { new Object[] { EasyMock.createStrictMock(SynchronizationContext.class) } };
	}
	
	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#cleanup(net.sf.hajdbc.sync.SynchronizationContextImpl)
	 */
	@SuppressWarnings("unchecked")
	@Test(dataProvider = "context")
	public <D> void cleanup(SynchronizationContext<D> context)
	{
		Database<D> sourceDatabase = EasyMock.createStrictMock(Database.class);
		Connection sourceConnection = EasyMock.createStrictMock(Connection.class);
		ExecutorService executor = Executors.newSingleThreadExecutor();
		
		EasyMock.expect(context.getActiveDatabaseSet()).andReturn(Collections.singleton(sourceDatabase));
		EasyMock.expect(context.getExecutor()).andReturn(executor);
		
		try
		{
			EasyMock.expect(context.getConnection(sourceDatabase)).andReturn(sourceConnection);
			
			sourceConnection.rollback();
			sourceConnection.setAutoCommit(true);
		}
		catch (SQLException e)
		{
			assert false : e;
		}
		
		EasyMock.replay(context, sourceDatabase, sourceConnection);
		
		this.strategy.cleanup(context);
		
		EasyMock.verify(context, sourceDatabase, sourceConnection);
	}

	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#prepare(net.sf.hajdbc.sync.SynchronizationContextImpl)
	 */
	@SuppressWarnings("unchecked")
	@Test(dataProvider = "context")
	public <D> void prepare(SynchronizationContext<D> context) throws SQLException
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
		ExecutorService executor = Executors.newSingleThreadExecutor();
		
		EasyMock.expect(context.getActiveDatabaseSet()).andReturn(Collections.singleton(sourceDatabase));
		EasyMock.expect(context.getExecutor()).andReturn(executor);
		
		EasyMock.checkOrder(context, false);
		EasyMock.checkOrder(metaData, false);
		EasyMock.checkOrder(database, false);
		EasyMock.checkOrder(sourceDatabase, false);
		EasyMock.checkOrder(sourceConnection, false);
		
		EasyMock.expect(context.getTargetDatabase()).andReturn(targetDatabase);
		EasyMock.expect(context.getConnection(targetDatabase)).andReturn(targetConnection);
		EasyMock.expect(context.getDatabaseMetaDataCache()).andReturn(metaData);
		EasyMock.expect(metaData.getDatabaseProperties(targetConnection)).andReturn(database);
		EasyMock.expect(database.getTables()).andReturn(Collections.singleton(table));
		
		EasyMock.expect(context.getConnection(sourceDatabase)).andReturn(sourceConnection);
		
		sourceConnection.setAutoCommit(false);
		sourceConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

		EasyMock.checkOrder(context, true);
		EasyMock.checkOrder(metaData, true);
		EasyMock.checkOrder(database, true);
		EasyMock.checkOrder(sourceDatabase, true);
		EasyMock.checkOrder(sourceConnection, true);
		
		EasyMock.expect(context.getDialect()).andReturn(dialect);

		EasyMock.expect(dialect.getLockTableSQL(table)).andReturn("LOCK TABLE table");
		
		EasyMock.expect(context.getConnection(sourceDatabase)).andReturn(sourceConnection);
		
		EasyMock.expect(sourceConnection.createStatement()).andReturn(statement);
		
		EasyMock.expect(statement.execute("LOCK TABLE table")).andReturn(true);
		
		statement.close();
		
		EasyMock.replay(context, sourceDatabase, targetDatabase, sourceConnection, targetConnection, statement, metaData, database, table, dialect);
		
		this.strategy.prepare(context);
		
		EasyMock.verify(context, sourceDatabase, targetDatabase, sourceConnection, targetConnection, statement, metaData, database, table, dialect);
	}
}
