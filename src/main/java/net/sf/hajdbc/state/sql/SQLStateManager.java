/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2009 Paul Ferraro
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
package net.sf.hajdbc.state.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.Set;
import java.util.TreeSet;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.durability.InvocationEvent;
import net.sf.hajdbc.durability.InvokerEvent;
import net.sf.hajdbc.pool.Pool;
import net.sf.hajdbc.pool.simple.SimplePool;
import net.sf.hajdbc.pool.simple.SimplePoolConfiguration;
import net.sf.hajdbc.pool.sql.ConnectionFactory;
import net.sf.hajdbc.pool.sql.ConnectionPoolProvider;
import net.sf.hajdbc.state.DatabaseEvent;
import net.sf.hajdbc.state.StateManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Paul Ferraro
 */
public class SQLStateManager implements StateManager, ConnectionFactory
{
	private static final String STATE_TABLE = "cluster_state";
	private static final String DATABASE_COLUMN = "database_id";

	private static final String INVOCATION_TABLE = "cluster_invocation";
	private static final String INVOKER_TABLE = "cluster_invoker";
	private static final String TRANSACTION_COLUMN = "tx_id";
	private static final String PHASE_COLUMN = "phase_id";
	private static final String SUCCESS_COLUMN = "success";
	private static final String RESULT_COLUMN = "result";
	
	static final String SELECT_STATE_SQL = MessageFormat.format("SELECT {1} FROM {0}", STATE_TABLE, DATABASE_COLUMN);
	static final String INSERT_STATE_SQL = MessageFormat.format("INSERT INTO {0} ({1}) VALUES (?)", STATE_TABLE, DATABASE_COLUMN);
	static final String DELETE_STATE_SQL = MessageFormat.format("DELETE FROM {0} WHERE {1} = ?", STATE_TABLE, DATABASE_COLUMN);
	static final String TRUNCATE_STATE_SQL = MessageFormat.format("DELETE FROM {0}", STATE_TABLE);

	static final String INSERT_INVOCATION_SQL = MessageFormat.format("INSERT INTO {0} ({1}, {2}) VALUES (?)", INVOCATION_TABLE, TRANSACTION_COLUMN, PHASE_COLUMN);
	static final String DELETE_INVOCATION_SQL = MessageFormat.format("DELETE FROM {0} WHERE {1} = ? AND {2} = ?", INVOCATION_TABLE, TRANSACTION_COLUMN, PHASE_COLUMN);
	
	static final String INSERT_INVOKER_SQL = MessageFormat.format("INSERT INTO {0} ({1}, {2}, {3}) VALUES (?, ?, ?)", INVOKER_TABLE, TRANSACTION_COLUMN, PHASE_COLUMN, DATABASE_COLUMN);
	static final String UPDATE_INVOKER_SQL = MessageFormat.format("UPDATE {0} SET {4} = ?, {5} = ? WHERE {1} = ? AND {2} = ? AND {3} = ?", INVOKER_TABLE, TRANSACTION_COLUMN, PHASE_COLUMN, DATABASE_COLUMN, SUCCESS_COLUMN, RESULT_COLUMN);
	static final String DELETE_INVOKER_SQL = MessageFormat.format("DELETE FROM {0} WHERE {1} = ? AND {2} = ?", INVOKER_TABLE, TRANSACTION_COLUMN, PHASE_COLUMN);

	private static final String CREATE_INVOKER_SQL = MessageFormat.format("CREATE TABLE IF NOT EXISTS {0} ({1} BINARY NOT NULL, {2} INTEGER NOT NULL, {3} VARCHAR NOT NULL, {4} BOOLEAN, {5} BINARY, PRIMARY KEY ({1}, {2}, {3}))", INVOKER_TABLE, TRANSACTION_COLUMN, PHASE_COLUMN, DATABASE_COLUMN, SUCCESS_COLUMN, RESULT_COLUMN);
	private static final String CREATE_STATE_SQL = MessageFormat.format("CREATE TABLE IF NOT EXISTS {0} ({1} VARCHAR NOT NULL, PRIMARY KEY ({1}))", STATE_TABLE, DATABASE_COLUMN);
	private static final String CREATE_INVOCATION_SQL = MessageFormat.format("CREATE TABLE IF NOT EXISTS {0} ({1} BINARY NOT NULL, {2} INTEGER NOT NULL, PRIMARY KEY ({1}))", INVOCATION_TABLE, TRANSACTION_COLUMN, PHASE_COLUMN);
	
	private static Logger logger = LoggerFactory.getLogger(SQLStateManager.class);
	
	private final String clusterId;
	
	private Pool<Connection, SQLException> pool;
	private String urlPattern = "jdbc:h2:{0}";
	private String user = "sa";
	private String password = "sa";

	private String url;
	
	public <Z, D extends Database<Z>> SQLStateManager(DatabaseCluster<Z, D> cluster)
	{
		this.clusterId = cluster.getId();
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.state.StateManager#isMembershipEmpty()
	 */
	@Override
	public boolean isMembershipEmpty()
	{
		return false;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.state.StateManager#getActiveDatabases()
	 */
	@Override
	public Set<String> getActiveDatabases()
	{
		Set<String> set = new TreeSet<String>();
		
		try
		{
			Connection connection = this.pool.take();
			
			try
			{
				PreparedStatement statement = connection.prepareStatement(SELECT_STATE_SQL);
				
				try
				{
					ResultSet resultSet = statement.executeQuery();
					
					try
					{
						while (resultSet.next())
						{
							set.add(resultSet.getString(1));
						}
					}
					finally
					{
						close(resultSet);
					}
				}
				finally
				{
					close(statement);
				}
			}
			finally
			{
				this.pool.release(connection);
			}
		}
		catch (SQLException e)
		{
			logger.error(e.getMessage(), e);
		}
		
		return set;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.state.StateManager#setActiveDatabases(java.util.Set)
	 */
	@Override
	public void setActiveDatabases(final Set<String> databases)
	{
		Transaction transaction = new Transaction()
		{
			@Override
			public void execute(Connection connection) throws SQLException
			{
				PreparedStatement statement = connection.prepareStatement(INSERT_STATE_SQL);
				
				try
				{
					statement.addBatch(TRUNCATE_STATE_SQL);
					
					for (String database: databases)
					{
						statement.clearParameters();
						
						statement.setString(1, database);
						
						statement.addBatch();
					}
					
					statement.executeBatch();
				}
				finally
				{
					close(statement);
				}
			}
		};

		try
		{
			this.execute(transaction);
		}
		catch (SQLException e)
		{
			// Log ERROR
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterListener#activated(net.sf.hajdbc.state.DatabaseEvent)
	 */
	@Override
	public void activated(final DatabaseEvent event)
	{
		Transaction transaction = new Transaction()
		{
			@Override
			public void execute(Connection connection) throws SQLException
			{
				SQLStateManager.this.execute(connection, INSERT_STATE_SQL, event);
			}
		};
		
		try
		{
			this.execute(transaction);
		}
		catch (SQLException e)
		{
			logger.error("", e);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseDeactivationListener#deactivated(net.sf.hajdbc.state.DatabaseEvent)
	 */
	@Override
	public void deactivated(final DatabaseEvent event)
	{
		Transaction transaction = new Transaction()
		{
			@Override
			public void execute(Connection connection) throws SQLException
			{
				SQLStateManager.this.execute(connection, DELETE_STATE_SQL, event);
			}
		};
		
		try
		{
			this.execute(transaction);
		}
		catch (SQLException e)
		{
			logger.error("", e);
		}
	}

	void execute(Connection connection, String sql, DatabaseEvent event) throws SQLException
	{
		PreparedStatement statement = connection.prepareStatement(sql);
		
		try
		{
			statement.setString(1, event.getDatabaseId());
			
			statement.executeUpdate();
		}
		finally
		{
			statement.close();
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.durability.DurabilityListener#beforeInvocation(net.sf.hajdbc.durability.InvocationEvent)
	 */
	@Override
	public void beforeInvocation(final InvocationEvent event)
	{
		Transaction transaction = new Transaction()
		{
			@Override
			public void execute(Connection connection) throws SQLException
			{
				SQLStateManager.this.execute(connection, INSERT_INVOCATION_SQL, event);
			}
		};
		
		try
		{
			this.execute(transaction);
		}
		catch (SQLException e)
		{
			logger.error("", e);
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.durability.DurabilityListener#afterInvocation(net.sf.hajdbc.durability.InvocationEvent)
	 */
	@Override
	public void afterInvocation(final InvocationEvent event)
	{
		Transaction transaction = new Transaction()
		{
			@Override
			public void execute(Connection connection) throws SQLException
			{
				SQLStateManager.this.execute(connection, DELETE_INVOKER_SQL, event);
				SQLStateManager.this.execute(connection, DELETE_INVOCATION_SQL, event);
			}
		};
		
		try
		{
			this.execute(transaction);
		}
		catch (SQLException e)
		{
			logger.error("", e);
		}
	}
	
	void execute(Connection connection, String sql, InvocationEvent event) throws SQLException 
	{
		PreparedStatement statement = connection.prepareStatement(sql);
		
		try
		{
			statement.setBytes(1, event.getTransactionId());
			statement.setInt(2, event.getPhase().ordinal());
			
			statement.executeUpdate();
		}
		finally
		{
			statement.close();
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.durability.DurabilityListener#beforeInvoker(net.sf.hajdbc.durability.InvokerEvent)
	 */
	@Override
	public void beforeInvoker(final InvokerEvent event)
	{
		Transaction transaction = new Transaction()
		{
			@Override
			public void execute(Connection connection) throws SQLException
			{
				PreparedStatement statement = connection.prepareStatement(INSERT_INVOKER_SQL);
				
				try
				{
					statement.setBytes(1, event.getTransactionId());
					statement.setInt(2, event.getPhase().ordinal());
					statement.setString(3, event.getDatabaseId());
					
					statement.executeUpdate();
				}
				finally
				{
					statement.close();
				}
			}
		};
		
		try
		{
			this.execute(transaction);
		}
		catch (SQLException e)
		{
			logger.error("", e);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.durability.DurabilityListener#afterInvoker(net.sf.hajdbc.durability.InvokerEvent)
	 */
	@Override
	public void afterInvoker(final InvokerEvent event)
	{
		Transaction transaction = new Transaction()
		{
			@Override
			public void execute(Connection connection) throws SQLException
			{
				PreparedStatement statement = connection.prepareStatement(UPDATE_INVOKER_SQL);
				
				try
				{
					statement.setBytes(1, event.getTransactionId());
					statement.setInt(2, event.getPhase().ordinal());
					statement.setString(3, event.getDatabaseId());
					
					byte[] exception = event.getException();
					boolean success = (exception == null);
					
					statement.setBoolean(4, success);
					statement.setBytes(5, success ? event.getResult() : exception);
					
					statement.executeUpdate();
				}
				finally
				{
					statement.close();
				}
			}
		};
		
		try
		{
			this.execute(transaction);
		}
		catch (SQLException e)
		{
			logger.error("", e);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.Lifecycle#start()
	 */
	@Override
	public void start() throws Exception
	{
		this.url = MessageFormat.format(this.urlPattern, this.clusterId);
		
		SimplePoolConfiguration configuration = new SimplePoolConfiguration();
		
		this.pool = new SimplePool<Connection, SQLException>(new ConnectionPoolProvider(this), configuration);
		
		this.pool.init();
		
		Connection connection = this.pool.take();
		
		boolean autoCommit = connection.getAutoCommit();
		
		connection.setAutoCommit(true);
		
		try
		{
			Statement statement = connection.createStatement();
			
			try
			{
				statement.addBatch(CREATE_STATE_SQL);
				statement.addBatch(CREATE_INVOCATION_SQL);
				statement.addBatch(CREATE_INVOKER_SQL);
				
				statement.executeBatch();
			}
			finally
			{
				close(statement);
			}
			
			connection.setAutoCommit(autoCommit);
		}
		finally
		{
			this.pool.release(connection);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.Lifecycle#stop()
	 */
	@Override
	public void stop()
	{
		this.pool.close();
	}

	private void execute(Transaction transaction) throws SQLException
	{
		Connection connection = this.pool.take();
		
		try
		{
			transaction.execute(connection);
			
			connection.commit();
		}
		finally
		{
			this.pool.release(connection);
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.pool.sql.ConnectionFactory#getConnection()
	 */
	@Override
	public Connection getConnection() throws SQLException
	{
		Connection connection = DriverManager.getConnection(this.url, this.user, this.password);
		
		connection.setAutoCommit(false);
		
		return connection;
	}

	interface Transaction
	{
		void execute(Connection connection) throws SQLException;
	}
	
	static void close(Statement statement)
	{
		try
		{
			statement.close();
		}
		catch (SQLException e)
		{
			logger.warn(e.getMessage(), e);
		}
	}
	
	static void close(ResultSet resultSet)
	{
		try
		{
			resultSet.close();
		}
		catch (SQLException e)
		{
			logger.warn(e.getMessage(), e);
		}
	}
}
