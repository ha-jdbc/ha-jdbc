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

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseMetaDataCache;
import net.sf.hajdbc.DatabaseProperties;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.SynchronizationContext;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.TableProperties;

/**
 * @author Paul Ferraro
 *
 */
public abstract class TestLockingSynchronizationStrategy implements SynchronizationStrategy
{
	protected IMocksControl control = EasyMock.createStrictControl();
	protected Database sourceDatabase = this.control.createMock(Database.class);
	protected Database targetDatabase = this.control.createMock(Database.class);
	protected Connection targetConnection = this.control.createMock(Connection.class);
	protected Connection sourceConnection = this.control.createMock(Connection.class);
	protected DatabaseMetaDataCache metaData = this.control.createMock(DatabaseMetaDataCache.class);
	protected Dialect dialect = this.control.createMock(Dialect.class);
	protected DatabaseProperties database = this.control.createMock(DatabaseProperties.class);
	protected TableProperties table = this.control.createMock(TableProperties.class);
	
	protected ExecutorService executor = Executors.newSingleThreadExecutor();
	
	protected SynchronizationStrategy strategy = this.createSynchronizationStrategy();

	protected abstract SynchronizationStrategy createSynchronizationStrategy();
	
	@DataProvider(name = "context")
	Object[][] contextProvider()
	{
		return new Object[][] { new Object[] { this.control.createMock(SynchronizationContext.class) } };
	}
	
	@AfterMethod
	void reset()
	{
		this.control.reset();
	}
	
	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#cleanup(net.sf.hajdbc.sync.SynchronizationContextImpl)
	 */
	@Test(dataProvider = "context")
	public void cleanup(SynchronizationContext context)
	{
		EasyMock.expect(context.getActiveDatabases()).andReturn(Collections.singleton(this.sourceDatabase));
		EasyMock.expect(context.getExecutor()).andReturn(this.executor);
		
		try
		{
			EasyMock.expect(context.getConnection(this.sourceDatabase)).andReturn(this.sourceConnection);
			
			this.sourceConnection.rollback();
			this.sourceConnection.setAutoCommit(true);
		}
		catch (SQLException e)
		{
			assert false : e;
		}
		
		this.control.replay();
		
		this.strategy.cleanup(context);
		
		this.control.verify();
	}

	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#prepare(net.sf.hajdbc.sync.SynchronizationContextImpl)
	 */
	@Test(dataProvider = "context")
	public void prepare(SynchronizationContext context) throws SQLException
	{
		Statement statement = this.control.createMock(Statement.class);
		
		EasyMock.expect(context.getActiveDatabases()).andReturn(Collections.singleton(this.sourceDatabase));
		EasyMock.expect(context.getExecutor()).andReturn(this.executor);
		
		this.control.checkOrder(false);
		
		EasyMock.expect(context.getTargetDatabase()).andReturn(this.targetDatabase);
		EasyMock.expect(context.getConnection(this.targetDatabase)).andReturn(this.targetConnection);
		EasyMock.expect(context.getDatabaseMetaDataCache()).andReturn(this.metaData);
		EasyMock.expect(this.metaData.getDatabaseProperties(this.targetConnection)).andReturn(this.database);
		EasyMock.expect(this.database.getTables()).andReturn(Collections.singleton(this.table));
		
		EasyMock.expect(context.getConnection(this.sourceDatabase)).andReturn(this.sourceConnection);
		
		this.sourceConnection.setAutoCommit(false);
		this.sourceConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

		this.control.checkOrder(true);
		
		EasyMock.expect(context.getDialect()).andReturn(this.dialect);

		EasyMock.expect(this.dialect.getLockTableSQL(this.table)).andReturn("LOCK TABLE table");
		
		EasyMock.expect(context.getConnection(this.sourceDatabase)).andReturn(this.sourceConnection);
		
		EasyMock.expect(this.sourceConnection.createStatement()).andReturn(statement);
		
		EasyMock.expect(statement.execute("LOCK TABLE table")).andReturn(true);
		
		statement.close();
		
		this.control.replay();
		
		this.strategy.prepare(context);
		
		this.control.verify();
	}
}
