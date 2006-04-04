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
package net.sf.hajdbc.sql;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.MockDatabase;
import net.sf.hajdbc.Operation;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.testng.annotations.Configuration;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;

/**
 * Unit test for {@link Statement}.
 * @author  Paul Ferraro
 * @since   1.1
 */
@Test
public class TestStatement
{
	protected IMocksControl control = EasyMock.createStrictControl();
	
	protected DatabaseCluster databaseCluster = this.control.createMock(DatabaseCluster.class);
	
	protected java.sql.Connection sqlConnection = this.control.createMock(java.sql.Connection.class);
	
	protected java.sql.Statement sqlStatement = this.control.createMock(this.getStatementClass());
	
	protected Database database = new MockDatabase();
	
	protected Balancer balancer = this.control.createMock(Balancer.class);
	
	protected FileSupport fileSupport = this.control.createMock(FileSupport.class);

	protected Lock lock = this.control.createMock(Lock.class);
	
	protected Dialect dialect = this.control.createMock(Dialect.class);
	
	protected Connection connection;
	protected Statement statement;
	protected List<Database> databaseList = Collections.singletonList(this.database);
	protected ExecutorService executor = Executors.newSingleThreadExecutor();
	
	protected Class<? extends java.sql.Statement> getStatementClass()
	{
		return java.sql.Statement.class;
	}
	
	@Configuration(beforeTestClass = true)
	protected void setUp() throws Exception
	{
		Map map = Collections.singletonMap(this.database, new Object());
		
		EasyMock.expect(this.databaseCluster.getConnectionFactoryMap()).andReturn(map);
		
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		this.lock.lock();
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		this.lock.unlock();

		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		this.lock.lock();
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		this.lock.unlock();
		
		this.control.replay();
		
		ConnectionFactory<Object> connectionFactory = new ConnectionFactory<Object>(this.databaseCluster, Object.class);
		
		Operation<Object, java.sql.Connection> operation = new Operation<Object, java.sql.Connection>()
		{
			public java.sql.Connection execute(Database database, Object object) throws SQLException
			{
				return TestStatement.this.sqlConnection;
			}
		};
		
		this.connection = new Connection(connectionFactory, operation, this.fileSupport);
		
		this.statement = this.createStatement(this.connection);
		
		this.control.verify();
		this.control.reset();
	}
	
	@Configuration(afterTestMethod = true)
	public void reset()
	{
		this.control.reset();
	}
	
	protected Statement createStatement(Connection connection) throws SQLException
	{
		Operation<java.sql.Connection, java.sql.Statement> operation = new Operation<java.sql.Connection, java.sql.Statement>()
		{
			public java.sql.Statement execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return TestStatement.this.sqlStatement;
			}
		};
		
		return new Statement(connection, operation);
	}
	
	/**
	 * Test method for {@link SQLObject#getObject(Database)}
	 */
	public void testGetObject()
	{
		this.control.replay();
		
		Object statement = this.statement.getObject(this.database);
		
		this.control.verify();
		
		assert this.sqlStatement == statement;
	}

	/**
	 * Test method for {@link SQLObject#getDatabaseCluster()}
	 */
	public void testGetDatabaseCluster()
	{
		this.control.replay();
		
		DatabaseCluster databaseCluster = this.statement.getDatabaseCluster();
		
		this.control.verify();
		
		assert this.databaseCluster == databaseCluster;
	}

	/**
	 * Test method for {@link SQLObject#handleExceptions(Map)}
	 */
	public void testHandleException()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlConnection.getAutoCommit()).andReturn(true);
			
			EasyMock.expect(this.databaseCluster.deactivate(this.database)).andReturn(false);
			
			this.control.replay();
			
			this.statement.handleExceptions(Collections.singletonMap(this.database, new SQLException()));
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link SQLObject#handleExceptions(Map)}
	 */
	public void testAutoCommitOffHandleException()
	{
		SQLException exception = new SQLException();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlConnection.getAutoCommit()).andReturn(false);
			
			this.databaseCluster.handleFailure(this.database, exception);
			
			this.control.replay();
			
			this.statement.handleExceptions(Collections.singletonMap(this.database, exception));
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}
	
	/**
	 * Test method for {@link Statement#addBatch(String)}
	 */
	public void testAddBatch()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlStatement.addBatch("test");
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.statement.addBatch("test");
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#cancel()}
	 */
	public void testCancel()
	{
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlStatement.cancel();
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.statement.cancel();
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#clearBatch()}
	 */
	public void testClearBatch()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlStatement.clearBatch();
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.statement.clearBatch();
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#clearWarnings()}
	 */
	public void testClearWarnings()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlStatement.clearWarnings();
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.statement.clearWarnings();
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#close()}
	 */
	public void testClose()
	{
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlStatement.close();
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.statement.close();
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#execute(String)}
	 */
	public void testExecuteString()
	{
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			EasyMock.expect(this.sqlStatement.execute("test")).andReturn(true);
			
			this.lock.unlock();
			
			this.control.replay();
			
			boolean results = this.statement.execute("test");
			
			this.control.verify();
			
			assert results;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#execute(String, int)}
	 */
	public void testExecuteStringInt()
	{
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			EasyMock.expect(this.sqlStatement.execute("SELECT ME", 1)).andReturn(true);
			
			this.lock.unlock();
			
			this.control.replay();
			
			boolean results = this.statement.execute("SELECT ME", 1);
			
			this.control.verify();
			
			assert results;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#execute(String, int[])}
	 */
	public void testExecuteStringIntArray()
	{
		int[] columns = new int[] { 0 };
		
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			EasyMock.expect(this.sqlStatement.execute("SELECT ME", columns)).andReturn(true);
			
			this.lock.unlock();
			
			this.control.replay();
			
			boolean results = this.statement.execute("SELECT ME", columns);
			
			this.control.verify();
			
			assert results;
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#execute(String, String[])}
	 */
	public void testExecuteStringStringArray()
	{
		String[] columns = new String[] { "column" };
		
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			EasyMock.expect(this.sqlStatement.execute("SELECT ME", columns)).andReturn(true);
			
			this.lock.unlock();
			
			this.control.replay();
			
			boolean results = this.statement.execute("SELECT ME", columns);
			
			this.control.verify();
			
			assert results;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#executeBatch()}
	 */
	public void testExecuteBatch()
	{
		int[] rows = new int[] { 100 };
		
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			EasyMock.expect(this.sqlStatement.executeBatch()).andReturn(rows);
			
			this.lock.unlock();
			
			this.control.replay();
			
			int[] results = this.statement.executeBatch();
			
			this.control.verify();
			
			assert rows == results;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#executeQuery(String)}
	 */
	public void testExecuteQuery()
	{
		ResultSet resultSet = EasyMock.createMock(ResultSet.class);
		DatabaseMetaData metaData = EasyMock.createMock(DatabaseMetaData.class);

		String sql = "SELECT ME";
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlStatement.getResultSetConcurrency()).andReturn(ResultSet.CONCUR_READ_ONLY);
			
			EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.next()).andReturn(this.database);
			
			this.balancer.beforeOperation(this.database);

			EasyMock.expect(this.sqlConnection.getMetaData()).andReturn(metaData);
			
			this.balancer.afterOperation(this.database);

			EasyMock.expect(this.dialect.isSelectForUpdate(metaData, sql)).andReturn(false);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.next()).andReturn(this.database);
			
			this.balancer.beforeOperation(this.database);
			
			EasyMock.expect(this.sqlStatement.executeQuery(sql)).andReturn(resultSet);
			
			this.balancer.afterOperation(this.database);
			
			this.control.replay();
			
			ResultSet rs = this.statement.executeQuery(sql);
			
			this.control.verify();
			
			assert resultSet == rs;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#executeQuery(String)}
	 */
	public void testUpdatableExecuteQuery()
	{
		ResultSet resultSet = EasyMock.createMock(ResultSet.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlStatement.getResultSetConcurrency()).andReturn(ResultSet.CONCUR_UPDATABLE);
			
			EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);		
			EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
			
			this.lock.lock();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			EasyMock.expect(this.sqlStatement.executeQuery("SELECT ME")).andReturn(resultSet);
			
			this.lock.unlock();
			
			this.control.replay();
			
			ResultSet rs = this.statement.executeQuery("SELECT ME");
			
			this.control.verify();
			
			assert net.sf.hajdbc.sql.ResultSet.class.cast(rs).getObject(this.database) == resultSet;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#executeQuery(String)}
	 */
	public void testSelectForUpdateExecuteQuery()
	{
		ResultSet resultSet = EasyMock.createMock(ResultSet.class);
		DatabaseMetaData metaData = EasyMock.createMock(DatabaseMetaData.class);

		String sql = "SELECT ME FOR UPDATE";
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlStatement.getResultSetConcurrency()).andReturn(ResultSet.CONCUR_READ_ONLY);
			
			EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.next()).andReturn(this.database);

			this.balancer.beforeOperation(this.database);
			
			EasyMock.expect(this.sqlConnection.getMetaData()).andReturn(metaData);
			
			this.balancer.afterOperation(this.database);
			
			EasyMock.expect(this.dialect.isSelectForUpdate(metaData, sql)).andReturn(true);
			
			EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
			EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
			
			this.lock.lock();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			EasyMock.expect(this.sqlStatement.executeQuery(sql)).andReturn(resultSet);
			
			this.lock.unlock();
			
			this.control.replay();
			
			ResultSet rs = this.statement.executeQuery(sql);
			
			this.control.verify();
			
			assert net.sf.hajdbc.sql.ResultSet.class.cast(rs).getObject(this.database) == resultSet;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#executeUpdate(String)}
	 */
	public void testExecuteUpdateString()
	{
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			EasyMock.expect(this.sqlStatement.executeUpdate("INSERT ME")).andReturn(1);
			
			this.lock.unlock();
			
			this.control.replay();
			
			int result = this.statement.executeUpdate("INSERT ME");
			
			this.control.verify();
			
			assert result == 1 : result;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#executeUpdate(String, int)}
	 */
	public void testExecuteUpdateStringInt()
	{
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			EasyMock.expect(this.sqlStatement.executeUpdate("INSERT INTO my_table (my_col) VALUES (}my_value})", 1)).andReturn(1);
			
			this.lock.unlock();
			
			this.control.replay();
			
			int result = this.statement.executeUpdate("INSERT INTO my_table (my_col) VALUES (}my_value})", 1);
			
			this.control.verify();
			
			assert result == 1 : result;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#executeUpdate(String, int[])}
	 */
	public void testExecuteUpdateStringIntArray()
	{
		int[] columns = new int[] { 0 };
		
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			EasyMock.expect(this.sqlStatement.executeUpdate("INSERT INTO my_table (my_col) VALUES (}my_value})", columns)).andReturn(1);
			
			this.lock.unlock();
			
			this.control.replay();
			
			int result = this.statement.executeUpdate("INSERT INTO my_table (my_col) VALUES (}my_value})", columns);
			
			this.control.verify();
			
			assert result == 1 : result;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#executeUpdate(String, String[])}
	 */
	public void testExecuteUpdateStringStringArray()
	{
		String[] columns = new String[] { "column" };
		
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			EasyMock.expect(this.sqlStatement.executeUpdate("INSERT INTO my_table (my_col) VALUES (}my_value})", columns)).andReturn(1);
			
			this.lock.unlock();
			
			this.control.replay();
			
			int result = this.statement.executeUpdate("INSERT INTO my_table (my_col) VALUES (}my_value})", columns);
			
			this.control.verify();
			
			assert result == 1 : result;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#getConnection()}
	 */
	public void testGetConnection()
	{
		java.sql.Connection connection = this.statement.getConnection();
		
		assert this.connection == connection;
	}

	/**
	 * Test method for {@link Statement#getFetchDirection()}
	 */
	public void testGetFetchDirection()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);		
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlStatement.getFetchDirection()).andReturn(ResultSet.FETCH_REVERSE);
			
			this.control.replay();
			
			int direction = this.statement.getFetchDirection();
			
			this.control.verify();
			
			assert direction == ResultSet.FETCH_REVERSE : direction;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#getFetchSize()}
	 */
	public void testGetFetchSize()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlStatement.getFetchSize()).andReturn(100);
			
			this.control.replay();
			
			int size = this.statement.getFetchSize();
			
			this.control.verify();
			
			assert size == 100 : size;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#getGeneratedKeys()}
	 */
	public void testGetGeneratedKeys()
	{
		ResultSet resultSet = EasyMock.createMock(ResultSet.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlStatement.getGeneratedKeys()).andReturn(resultSet);
			
			this.control.replay();
			
			ResultSet rs = this.statement.getGeneratedKeys();
			
			this.control.verify();
			
			assert rs == resultSet;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#getMaxFieldSize()}
	 */
	public void testGetMaxFieldSize()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlStatement.getMaxFieldSize()).andReturn(100);
			
			this.control.replay();
			
			int size = this.statement.getMaxFieldSize();
			
			this.control.verify();
			
			assert size == 100 : size;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#getMaxRows()}
	 */
	public void testGetMaxRows()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlStatement.getMaxRows()).andReturn(100);
			
			this.control.replay();
			
			int size = this.statement.getMaxRows();
			
			this.control.verify();
			
			assert size == 100 : size;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#getMoreResults()}
	 */
	public void testGetMoreResults()
	{
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			EasyMock.expect(this.sqlStatement.getMoreResults()).andReturn(true);
			
			this.lock.unlock();
			
			this.control.replay();
			
			boolean more = this.statement.getMoreResults();
			
			this.control.verify();
			
			assert more;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#getMoreResults(int)}
	 */
	public void testGetMoreResultsInt()
	{
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			EasyMock.expect(this.sqlStatement.getMoreResults(Statement.CLOSE_CURRENT_RESULT)).andReturn(true);
			
			this.lock.unlock();
			
			this.control.replay();
			
			boolean more = this.statement.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
			
			this.control.verify();
			
			assert more;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#getMoreResults(int)}
	 */
	public void testKeepOpenGetMoreResultsInt()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			EasyMock.expect(this.sqlStatement.getMoreResults(Statement.KEEP_CURRENT_RESULT)).andReturn(true);
			
			this.lock.unlock();
			
			this.control.replay();
			
			boolean more = this.statement.getMoreResults(Statement.KEEP_CURRENT_RESULT);
			
			this.control.verify();
			
			assert more;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#getQueryTimeout()}
	 */
	public void testGetQueryTimeout()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlStatement.getQueryTimeout()).andReturn(100);
			
			this.control.replay();
			
			int timeout = this.statement.getQueryTimeout();
			
			this.control.verify();
			
			assert timeout == 100 : timeout;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#getResultSet()}
	 */
	public void testGetResultSet()
	{
		ResultSet resultSet = EasyMock.createMock(ResultSet.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlStatement.getGeneratedKeys()).andReturn(resultSet);
			
			this.control.replay();
			
			ResultSet rs = this.statement.getGeneratedKeys();
			
			this.control.verify();
			
			assert rs == resultSet;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#getResultSetConcurrency()}
	 */
	public void testGetResultSetConcurrency()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlStatement.getResultSetConcurrency()).andReturn(ResultSet.CONCUR_UPDATABLE);
			
			this.control.replay();
			
			int concurrency = this.statement.getResultSetConcurrency();
			
			this.control.verify();
			
			assert concurrency == ResultSet.CONCUR_UPDATABLE : concurrency;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#getResultSetHoldability()}
	 */
	public void testGetResultSetHoldability()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);		
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlStatement.getResultSetConcurrency()).andReturn(ResultSet.HOLD_CURSORS_OVER_COMMIT);
			
			this.control.replay();
			
			int holdability = this.statement.getResultSetConcurrency();
			
			this.control.verify();
			
			assert holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT : holdability;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#getResultSetType()}
	 */
	public void testGetResultSetType()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);		
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlStatement.getResultSetType()).andReturn(ResultSet.TYPE_SCROLL_SENSITIVE);
			
			this.control.replay();
			
			int type = this.statement.getResultSetType();
			
			this.control.verify();
			
			assert type == ResultSet.TYPE_SCROLL_SENSITIVE : type;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#getUpdateCount()}
	 */
	public void testGetUpdateCount()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlStatement.getUpdateCount()).andReturn(100);
			
			this.control.replay();
			
			int count = this.statement.getUpdateCount();
			
			this.control.verify();
			
			assert count == 100;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#getWarnings()}
	 */
	public void testGetWarnings()
	{
		SQLWarning warnings = new SQLWarning();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);		
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlStatement.getWarnings()).andReturn(warnings);
			
			this.control.replay();
			
			SQLWarning warn = this.statement.getWarnings();
			
			this.control.verify();
			
			assert warn == warnings;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#setCursorName(String)}
	 */
	public void testSetCursorName()
	{
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlStatement.setCursorName("test");
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.statement.setCursorName("test");
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#setEscapeProcessing(boolean)}
	 */
	public void testSetEscapeProcessing()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlStatement.setEscapeProcessing(true);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.statement.setEscapeProcessing(true);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#setFetchDirection(int)}
	 */
	public void testSetFetchDirection()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlStatement.setFetchDirection(ResultSet.FETCH_REVERSE);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.statement.setFetchDirection(ResultSet.FETCH_REVERSE);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#setFetchSize(int)}
	 */
	public void testSetFetchSize()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlStatement.setFetchSize(100);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.statement.setFetchSize(100);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#setMaxFieldSize(int)}
	 */
	public void testSetMaxFieldSize()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlStatement.setMaxFieldSize(100);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.statement.setMaxFieldSize(100);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#setMaxRows(int)}
	 */
	public void testSetMaxRows()
	{
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlStatement.setMaxRows(100);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.statement.setMaxRows(100);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Statement#setQueryTimeout(int)}
	 */
	public void testSetQueryTimeout()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlStatement.setQueryTimeout(100);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.statement.setQueryTimeout(100);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}
}
