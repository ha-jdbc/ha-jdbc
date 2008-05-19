/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2008 Paul Ferraro
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
package net.sf.hajdbc.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.LockManager;

import org.easymock.EasyMock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
@Test
@SuppressWarnings("unchecked")
public class TestLocalTransactionContext implements TransactionContext
{
	private Connection connection = EasyMock.createStrictMock(Connection.class);
	private DatabaseCluster cluster = EasyMock.createStrictMock(DatabaseCluster.class);
	private InvocationStrategy strategy = EasyMock.createStrictMock(InvocationStrategy.class);
	private LockManager lockManager = EasyMock.createStrictMock(LockManager.class);
	private Lock lock = EasyMock.createStrictMock(Lock.class);
	private TransactionContext context;
	
	private Object[] objects()
	{
		return new Object[] { this.connection, this.cluster, this.strategy, this.lock, this.lockManager };
	}
	
	@BeforeMethod
	void init()
	{
		EasyMock.expect(this.cluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.replay();
		
		this.context = new LocalTransactionContext(this.cluster);
		
		this.verify();
		this.reset();
	}
	
	void replay()
	{
		EasyMock.replay(this.objects());
	}
	
	void verify()
	{
		EasyMock.verify(this.objects());
	}
	
	@AfterMethod
	void reset()
	{
		EasyMock.reset(this.objects());		
	}
	
	@DataProvider(name = "start")
	Object[][] startParameters()
	{
		return new Object[][] { new Object[] { this.strategy, this.connection } };
	}
	
	@Test(dataProvider = "start")
	public void testStart(InvocationStrategy strategy, Connection connection) throws SQLException
	{
		SQLProxy proxy = EasyMock.createMock(SQLProxy.class);
		Invoker invoker = EasyMock.createMock(Invoker.class);
		
		// Auto-commit on
		EasyMock.expect(this.connection.getAutoCommit()).andReturn(true);
		
		this.replay();
		
		InvocationStrategy result = this.start(strategy, connection);
		
		this.verify();
		
		assert result != null;
		assert result != strategy;
		assert LockingInvocationStrategy.class.isInstance(result) : result.getClass().getName();
		
		this.reset();
		
		try
		{
			Object object = new Object();
			
			this.lock.lock();
			
			EasyMock.expect(strategy.invoke(proxy, invoker)).andReturn(object);
			
			this.lock.unlock();
			
			this.replay();
			
			Object invocationResult = result.invoke(proxy, invoker);
			
			this.verify();
			
			assert invocationResult == object;
			
			this.reset();
		}
		catch (Exception e)
		{
			assert false : e;
		}
		
		// Auto-commit off
		EasyMock.expect(this.connection.getAutoCommit()).andReturn(false);
		
		this.replay();
		
		result = this.start(strategy, connection);
		
		this.verify();
		
		assert result != null;
		assert result != strategy;
		assert !LockingInvocationStrategy.class.isInstance(result);
		
		this.reset();
		
		try
		{
			Object object = new Object();
			
			this.lock.lock();
			
			EasyMock.expect(strategy.invoke(proxy, invoker)).andReturn(object);
			
			this.replay();
			
			Object invocationResult = result.invoke(proxy, invoker);
			
			this.verify();
			
			assert invocationResult == object;
			
			this.reset();
		}
		catch (Exception e)
		{
			assert false : e;
		}
		
		// Already locked
		this.replay();
		
		result = this.start(strategy, connection);
		
		this.verify();
		
		assert result == strategy;
	}
	
	@Override
	public InvocationStrategy start(InvocationStrategy strategy, Connection connection) throws SQLException
	{
		return this.context.start(strategy, connection);
	}
	
	@DataProvider(name = "end")
	Object[][] endParameters()
	{
		return new Object[][] { new Object[] { this.strategy } };
	}

	@Test(dataProvider = "end")
	public void testEnd(InvocationStrategy strategy) throws SQLException
	{
		// Not locked
		this.replay();
		
		InvocationStrategy result = this.end(strategy);

		this.verify();
		
		assert result == strategy;
		
		this.reset();
		
		// Simulate transaction start
		EasyMock.expect(this.connection.getAutoCommit()).andReturn(false);
		
		this.replay();
		
		result = this.start(this.strategy, this.connection);
		
		this.verify();
		this.reset();
		
		SQLProxy proxy = EasyMock.createMock(SQLProxy.class);
		Invoker invoker = EasyMock.createMock(Invoker.class);

		Object object = new Object();
		
		try
		{
			this.lock.lock();
			
			EasyMock.expect(this.strategy.invoke(proxy, invoker)).andReturn(object);
			
			this.replay();
			
			Object invocationResult = result.invoke(proxy, invoker);
			
			this.verify();
			
			assert invocationResult == object;
			
			this.reset();
		}
		catch (Exception e)
		{
			assert false : e;
		}
		
		// Locked
		this.replay();
		
		result = this.end(strategy);
		
		this.verify();
		
		assert result != strategy;
		
		this.reset();
		
		try
		{
			EasyMock.expect(strategy.invoke(proxy, invoker)).andReturn(object);
			
			this.lock.unlock();
			
			this.replay();
			
			Object invocationResult = result.invoke(proxy, invoker);
			
			this.verify();
			
			assert invocationResult == object;
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}
	
	@Override
	public InvocationStrategy end(InvocationStrategy strategy) throws SQLException
	{
		return this.context.end(strategy);
	}

	@Override
	public void close()
	{
		// Normally uneventful
		this.replay();
		
		this.close();
		
		this.verify();
		this.reset();
		
		// Simulate transaction start
		try
		{
			EasyMock.expect(this.connection.getAutoCommit()).andReturn(false);
			
			this.replay();
			
			InvocationStrategy result = this.start(this.strategy, this.connection);
			
			this.verify();
			this.reset();
			
			SQLProxy proxy = EasyMock.createMock(SQLProxy.class);
			Invoker invoker = EasyMock.createMock(Invoker.class);

			Object object = new Object();
			
			this.lock.lock();
			
			EasyMock.expect(this.strategy.invoke(proxy, invoker)).andReturn(object);
			
			this.replay();
			
			Object invocationResult = result.invoke(proxy, invoker);
			
			this.verify();
			
			assert invocationResult == object;
			
			this.reset();
		}
		catch (Exception e)
		{
			assert false : e;
		}
		
		// Closing with uncommitted transaction
		this.lock.unlock();
		
		this.replay();
		
		this.close();
		
		this.verify();
	}
}