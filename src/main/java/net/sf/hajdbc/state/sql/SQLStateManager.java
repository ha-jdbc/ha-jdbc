/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc.state.sql;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseProperties;
import net.sf.hajdbc.ExceptionType;
import net.sf.hajdbc.IdentifiableMatcher;
import net.sf.hajdbc.cache.lazy.LazyDatabaseProperties;
import net.sf.hajdbc.cache.simple.SimpleDatabaseMetaDataProvider;
import net.sf.hajdbc.dialect.Dialect;
import net.sf.hajdbc.dialect.DialectFactory;
import net.sf.hajdbc.durability.Durability;
import net.sf.hajdbc.durability.DurabilityListener;
import net.sf.hajdbc.durability.InvocationEvent;
import net.sf.hajdbc.durability.InvocationEventImpl;
import net.sf.hajdbc.durability.InvokerEvent;
import net.sf.hajdbc.durability.InvokerEventImpl;
import net.sf.hajdbc.durability.InvokerResult;
import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;
import net.sf.hajdbc.pool.Pool;
import net.sf.hajdbc.pool.PoolFactory;
import net.sf.hajdbc.pool.sql.ConnectionFactory;
import net.sf.hajdbc.pool.sql.ConnectionPoolProvider;
import net.sf.hajdbc.sql.DriverDatabase;
import net.sf.hajdbc.state.DatabaseEvent;
import net.sf.hajdbc.state.DurabilityListenerAdapter;
import net.sf.hajdbc.state.SerializedDurabilityListener;
import net.sf.hajdbc.state.StateManager;
import net.sf.hajdbc.tx.TransactionIdentifierFactory;
import net.sf.hajdbc.util.Objects;
import net.sf.hajdbc.util.Resources;
import net.sf.hajdbc.util.ServiceLoaders;

/**
 * @author Paul Ferraro
 */
public class SQLStateManager<Z, D extends Database<Z>> implements StateManager, ConnectionFactory, SerializedDurabilityListener
{
	private static final String STATE_TABLE = "cluster_state";
	private static final String DATABASE_COLUMN = "database_id";

	private static final String INVOCATION_TABLE = "cluster_invocation";
	private static final String INVOKER_TABLE = "cluster_invoker";
	private static final String TRANSACTION_COLUMN = "tx_id";
	private static final String PHASE_COLUMN = "phase_id";
	private static final String EXCEPTION_COLUMN = "exception_id";
	private static final String RESULT_COLUMN = "result";
	
	static final String SELECT_STATE_SQL = MessageFormat.format("SELECT {1} FROM {0}", STATE_TABLE, DATABASE_COLUMN);
	static final String INSERT_STATE_SQL = MessageFormat.format("INSERT INTO {0} ({1}) VALUES (?)", STATE_TABLE, DATABASE_COLUMN);
	static final String DELETE_STATE_SQL = MessageFormat.format("DELETE FROM {0} WHERE {1} = ?", STATE_TABLE, DATABASE_COLUMN);
	static final String TRUNCATE_STATE_SQL = MessageFormat.format("DELETE FROM {0}", STATE_TABLE);

	static final String SELECT_INVOCATION_SQL = MessageFormat.format("SELECT {1}, {2}, {3} FROM {0}", INVOCATION_TABLE, TRANSACTION_COLUMN, PHASE_COLUMN, EXCEPTION_COLUMN);
	static final String INSERT_INVOCATION_SQL = MessageFormat.format("INSERT INTO {0} ({1}, {2}, {3}) VALUES (?, ?, ?)", INVOCATION_TABLE, TRANSACTION_COLUMN, PHASE_COLUMN, EXCEPTION_COLUMN);
	static final String DELETE_INVOCATION_SQL = MessageFormat.format("DELETE FROM {0} WHERE {1} = ? AND {2} = ?", INVOCATION_TABLE, TRANSACTION_COLUMN, PHASE_COLUMN);
	
	static final String SELECT_INVOKER_SQL = MessageFormat.format("SELECT {1}, {2}, {3}, {4} FROM {0}", INVOKER_TABLE, TRANSACTION_COLUMN, PHASE_COLUMN, DATABASE_COLUMN, RESULT_COLUMN);
	static final String INSERT_INVOKER_SQL = MessageFormat.format("INSERT INTO {0} ({1}, {2}, {3}) VALUES (?, ?, ?)", INVOKER_TABLE, TRANSACTION_COLUMN, PHASE_COLUMN, DATABASE_COLUMN);
	static final String UPDATE_INVOKER_SQL = MessageFormat.format("UPDATE {0} SET {4} = ? WHERE {1} = ? AND {2} = ? AND {3} = ?", INVOKER_TABLE, TRANSACTION_COLUMN, PHASE_COLUMN, DATABASE_COLUMN, RESULT_COLUMN);
	static final String DELETE_INVOKER_SQL = MessageFormat.format("DELETE FROM {0} WHERE {1} = ? AND {2} = ?", INVOKER_TABLE, TRANSACTION_COLUMN, PHASE_COLUMN);

	private static final String CREATE_INVOCATION_SQL = MessageFormat.format("CREATE TABLE {0} ({1} {2} NOT NULL, {3} {4} NOT NULL, {5} {6} NOT NULL, PRIMARY KEY ({1}, {3}))", INVOCATION_TABLE, TRANSACTION_COLUMN, "{0}", PHASE_COLUMN, "{1}", EXCEPTION_COLUMN, "{2}");
	private static final String CREATE_INVOKER_SQL = MessageFormat.format("CREATE TABLE {0} ({1} {2} NOT NULL, {3} {4} NOT NULL, {5} {6} NOT NULL, {7} {8}, PRIMARY KEY ({1}, {3}, {5}))", INVOKER_TABLE, TRANSACTION_COLUMN, "{0}", PHASE_COLUMN, "{1}", DATABASE_COLUMN, "{2}", RESULT_COLUMN, "{3}");
	private static final String CREATE_STATE_SQL = MessageFormat.format("CREATE TABLE {0} ({1} {2} NOT NULL, PRIMARY KEY ({1}))", STATE_TABLE, DATABASE_COLUMN, "{0}");
	
	private static Logger logger = LoggerFactory.getLogger(SQLStateManager.class);
	
	private final DurabilityListener listener;
	private final DatabaseCluster<Z, D> cluster;
	private final PoolFactory poolFactory;
	private final DriverDatabase database;
	
	private String password;
	private Driver driver;
	private Pool<Connection, SQLException> pool;
	
	public SQLStateManager(DatabaseCluster<Z, D> cluster, DriverDatabase database, PoolFactory poolFactory)
	{
		this.cluster = cluster;
		this.database = database;
		this.poolFactory = poolFactory;
		this.listener = new DurabilityListenerAdapter(this, cluster.getTransactionIdentifierFactory());
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.state.StateManager#getActiveDatabases()
	 */
	@Override
	public Set<String> getActiveDatabases()
	{
		Query<Set<String>> query = new Query<Set<String>>()
		{
			@Override
			public Set<String> execute(Connection connection) throws SQLException
			{
				Set<String> set = new TreeSet<String>();
				
				PreparedStatement statement = connection.prepareStatement(SELECT_STATE_SQL);
				
				try
				{
					ResultSet resultSet = statement.executeQuery();
					
					while (resultSet.next())
					{
						set.add(resultSet.getString(1));
					}
					
					return set;
				}
				finally
				{
					Resources.close(statement);
				}
			}
		};

		try
		{
			return this.execute(query);
		}
		catch (SQLException e)
		{
			logger.log(Level.ERROR, e, e.getMessage());
			return Collections.emptySet();
		}
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
				Statement s = connection.createStatement();
				
				try
				{
					s.executeUpdate(TRUNCATE_STATE_SQL);
				}
				finally
				{
					Resources.close(s);
				}
				
				PreparedStatement statement = connection.prepareStatement(INSERT_STATE_SQL);
				
				try
				{
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
					Resources.close(statement);
				}
			}
		};

		try
		{
			this.execute(transaction);
		}
		catch (SQLException e)
		{
			logger.log(Level.ERROR, e, e.getMessage());
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
			logger.log(Level.ERROR, e, e.getMessage());
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterListener#deactivated(net.sf.hajdbc.state.DatabaseEvent)
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
			logger.log(Level.ERROR, e, e.getMessage());
		}
	}

	void execute(Connection connection, String sql, DatabaseEvent event) throws SQLException
	{
		PreparedStatement statement = connection.prepareStatement(sql);
		
		try
		{
			statement.setString(1, event.getSource());
			
			statement.executeUpdate();
		}
		finally
		{
			Resources.close(statement);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.state.SerializedDurabilityListener#beforeInvocation(byte[], byte, byte)
	 */
	@Override
	public void beforeInvocation(final byte[] transactionId, final byte phase, final byte exceptionType)
	{
		Transaction transaction = new Transaction()
		{
			@Override
			public void execute(Connection connection) throws SQLException
			{
				PreparedStatement statement = connection.prepareStatement(INSERT_INVOCATION_SQL);
				
				try
				{
					statement.setBytes(1, transactionId);
					statement.setByte(2, phase);
					statement.setByte(3, exceptionType);
					
					statement.executeUpdate();
				}
				finally
				{
					Resources.close(statement);
				}
			}
		};
		
		try
		{
			this.execute(transaction);
		}
		catch (SQLException e)
		{
			logger.log(Level.ERROR, e, e.getMessage());
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.state.SerializedDurabilityListener#afterInvocation(byte[], byte)
	 */
	@Override
	public void afterInvocation(final byte[] transactionId, final byte phase)
	{
		Transaction transaction = new Transaction()
		{
			@Override
			public void execute(Connection connection) throws SQLException
			{
				SQLStateManager.this.execute(connection, DELETE_INVOKER_SQL, transactionId, phase);
				SQLStateManager.this.execute(connection, DELETE_INVOCATION_SQL, transactionId, phase);
			}
		};
		
		try
		{
			this.execute(transaction);
		}
		catch (SQLException e)
		{
			logger.log(Level.ERROR, e, e.getMessage());
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.state.SerializedDurabilityListener#beforeInvoker(byte[], byte, java.lang.String)
	 */
	@Override
	public void beforeInvoker(final byte[] transactionId, final byte phase, final String databaseId)
	{
		Transaction transaction = new Transaction()
		{
			@Override
			public void execute(Connection connection) throws SQLException
			{
				PreparedStatement statement = connection.prepareStatement(INSERT_INVOKER_SQL);
				
				try
				{
					statement.setBytes(1, transactionId);
					statement.setByte(2, phase);
					statement.setString(3, databaseId);
					
					statement.executeUpdate();
				}
				finally
				{
					Resources.close(statement);
				}
			}
		};
		
		try
		{
			this.execute(transaction);
		}
		catch (SQLException e)
		{
			logger.log(Level.ERROR, e, e.getMessage());
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.state.SerializedDurabilityListener#afterInvoker(byte[], byte, java.lang.String, byte[])
	 */
	@Override
	public void afterInvoker(final byte[] transactionId, final byte phase, final String databaseId, final byte[] result)
	{
		Transaction transaction = new Transaction()
		{
			@Override
			public void execute(Connection connection) throws SQLException
			{
				PreparedStatement statement = connection.prepareStatement(UPDATE_INVOKER_SQL);
				
				try
				{
					statement.setBytes(1, result);
					statement.setBytes(2, transactionId);
					statement.setByte(3, phase);
					statement.setString(4, databaseId);
					
					statement.executeUpdate();
				}
				finally
				{
					Resources.close(statement);
				}
			}
		};
		
		try
		{
			this.execute(transaction);
		}
		catch (SQLException e)
		{
			logger.log(Level.ERROR, e, e.getMessage());
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.durability.DurabilityListener#beforeInvocation(net.sf.hajdbc.durability.InvocationEvent)
	 */
	@Override
	public void beforeInvocation(InvocationEvent event)
	{
		this.listener.beforeInvocation(event);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.durability.DurabilityListener#afterInvocation(net.sf.hajdbc.durability.InvocationEvent)
	 */
	@Override
	public void afterInvocation(InvocationEvent event)
	{
		this.listener.afterInvocation(event);
	}
	
	void execute(Connection connection, String sql, byte[] transactionId, byte phase) throws SQLException 
	{
		PreparedStatement statement = connection.prepareStatement(sql);
		
		try
		{
			statement.setBytes(1, transactionId);
			statement.setByte(2, phase);
			
			statement.executeUpdate();
		}
		finally
		{
			Resources.close(statement);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.state.StateManager#recover()
	 */
	@Override
	public Map<InvocationEvent, Map<String, InvokerEvent>> recover()
	{
		final TransactionIdentifierFactory<?> txIdFactory = this.cluster.getTransactionIdentifierFactory();
		Query<Map<InvocationEvent, Map<String, InvokerEvent>>> query = new Query<Map<InvocationEvent, Map<String, InvokerEvent>>>()
		{
			@Override
			public Map<InvocationEvent, Map<String, InvokerEvent>> execute(Connection connection) throws SQLException
			{
				Map<InvocationEvent, Map<String, InvokerEvent>> map = new HashMap<InvocationEvent, Map<String, InvokerEvent>>();
				
				PreparedStatement statement = connection.prepareStatement(SELECT_INVOCATION_SQL);

				try
				{
					ResultSet resultSet = statement.executeQuery();
					while (resultSet.next())
					{
						Object txId = txIdFactory.deserialize(resultSet.getBytes(1));
						Durability.Phase phase = Durability.Phase.values()[resultSet.getInt(2)];
						ExceptionType type = ExceptionType.values()[resultSet.getInt(3)];
						map.put(new InvocationEventImpl(txId, phase, type), new HashMap<String, InvokerEvent>());
					}
				}
				finally
				{
					Resources.close(statement);
				}

				statement = connection.prepareStatement(SELECT_INVOKER_SQL);

				try
				{
					ResultSet resultSet = statement.executeQuery();
					
					while (resultSet.next())
					{
						Object txId = txIdFactory.deserialize(resultSet.getBytes(1));
						Durability.Phase phase = Durability.Phase.values()[resultSet.getByte(2)];
						
						Map<String, InvokerEvent> invokers = map.get(new InvocationEventImpl(txId, phase, null));
						
						if (invokers != null)
						{
							String databaseId = resultSet.getString(3);
							
							InvokerEvent event = new InvokerEventImpl(txId, phase, databaseId);
							
							byte[] bytes = resultSet.getBytes(4);
							
							if (!resultSet.wasNull())
							{
								event.setResult(Objects.<InvokerResult>deserialize(bytes));
							}

							invokers.put(databaseId, event);
						}
					}
				}
				finally
				{
					Resources.close(statement);
				}
				
				return map;
			}
		};
		
		try
		{
			return this.execute(query);
		}
		catch (SQLException e)
		{
			throw new IllegalStateException(e);
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.durability.DurabilityListener#beforeInvoker(net.sf.hajdbc.durability.InvokerEvent)
	 */
	@Override
	public void beforeInvoker(InvokerEvent event)
	{
		this.listener.beforeInvoker(event);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.durability.DurabilityListener#afterInvoker(net.sf.hajdbc.durability.InvokerEvent)
	 */
	@Override
	public void afterInvoker(InvokerEvent event)
	{
		this.listener.afterInvoker(event);
	}

	@Override
	public boolean isEnabled()
	{
		return true;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.Lifecycle#start()
	 */
	@Override
	public void start() throws Exception
	{
		this.driver = this.database.getConnectionSource();
		this.password = this.database.decodePassword(this.cluster.getDecoder());
		this.pool = this.poolFactory.createPool(new ConnectionPoolProvider(this));
		
		DialectFactory factory = ServiceLoaders.findService(new IdentifiableMatcher<DialectFactory>(this.database.parseVendor()), DialectFactory.class);
		if (factory == null)
		{
			// Use default dialect
			factory = ServiceLoaders.findRequiredService(DialectFactory.class);
		}
		
		Dialect dialect = factory.createDialect();

		Connection connection = this.pool.take();
		
		try
		{
			connection.setAutoCommit(true);
	
			DatabaseProperties properties = new LazyDatabaseProperties(new SimpleDatabaseMetaDataProvider(connection.getMetaData()), dialect);

			String enumType = properties.findType(0, Types.TINYINT, Types.SMALLINT, Types.INTEGER);
			String stringType = properties.findType(Database.ID_MAX_SIZE, Types.VARCHAR);
			String binaryType = properties.findType(this.cluster.getTransactionIdentifierFactory().size(), Types.BINARY);
			String varBinaryType = properties.findType(0, Types.VARBINARY);
			
			Statement statement = connection.createStatement();
			
			try
			{
				createTableIfNotExists(statement, properties, STATE_TABLE, CREATE_STATE_SQL, stringType);
				createTableIfNotExists(statement, properties, INVOCATION_TABLE, CREATE_INVOCATION_SQL, binaryType, enumType, enumType);
				createTableIfNotExists(statement, properties, INVOKER_TABLE, CREATE_INVOKER_SQL, binaryType, enumType, stringType, varBinaryType);

				if (Boolean.getBoolean(StateManager.CLEAR_LOCAL_STATE))
				{
					statement.executeUpdate(TRUNCATE_STATE_SQL);
				}
			}
			finally
			{
				Resources.close(statement);
			}
		}
		finally
		{
			this.pool.release(connection);
		}
	}

	private static void createTableIfNotExists(Statement statement, DatabaseProperties properties, String table, String pattern, String... types) throws SQLException
	{
		if (properties.findTable(table) == null)
		{
			String sql = MessageFormat.format(pattern, (Object[]) types);
			logger.log(Level.DEBUG, sql);
			statement.executeUpdate(sql);
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.Lifecycle#stop()
	 */
	@Override
	public void stop()
	{
		if (this.pool != null)
		{
			this.pool.close();
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.pool.sql.ConnectionFactory#getConnection()
	 */
	@Override
	public Connection getConnection() throws SQLException
	{
		Connection connection = this.database.connect(this.driver, this.cluster.getDecoder().decode(this.password));
		
		connection.setAutoCommit(false);
		
		return connection;
	}

	private <T> T execute(Query<T> query) throws SQLException
	{
		Connection connection = this.pool.take();
		
		try
		{
			return query.execute(connection);
		}
		finally
		{
			this.pool.release(connection);
		}
	}

	interface Query<T>
	{
		T execute(Connection connection) throws SQLException;
	}

	private void execute(Transaction transaction) throws SQLException
	{
		Connection connection = this.pool.take();
		
		try
		{
			transaction.execute(connection);
			
			connection.commit();
		}
		catch (SQLException e)
		{
			try
			{
				connection.rollback();
			}
			catch (SQLException ex)
			{
				logger.log(Level.WARN, ex);
			}
			throw e;
		}
		finally
		{
			this.pool.release(connection);
		}
	}

	interface Transaction
	{
		void execute(Connection connection) throws SQLException;
	}
}
