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
package net.sf.hajdbc.local;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.sql.DataSource;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseClusterFactory;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.SQLException;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.SynchronizationStrategyBuilder;
import net.sf.hajdbc.util.concurrent.DaemonThreadFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class LocalDatabaseCluster implements DatabaseCluster
{
	private static final String STATE_DELIMITER = ",";
	
	private static Preferences preferences = Preferences.userNodeForPackage(LocalDatabaseCluster.class);
	static Log log = LogFactory.getLog(LocalDatabaseCluster.class);
	
	private String id;
	private Map<String, Database> databaseMap = new HashMap<String, Database>();
	private Balancer balancer;
	private String defaultSynchronizationStrategyId;
	private Map<Database, Object> connectionFactoryMap = new HashMap<Database, Object>();
	private ThreadPoolExecutor executor = ThreadPoolExecutor.class.cast(Executors.newCachedThreadPool());
	private Dialect dialect;
	private ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(1, new DaemonThreadFactory(Thread.MIN_PRIORITY));
	private int failureDetectionPeriod;
	private ReadWriteLock lock = this.createReadWriteLock();
	
	/**
	 * Work around for missing constructor in backport-util-concurrent package.
	 * @return ReadWriteLock implementation
	 */
	private ReadWriteLock createReadWriteLock()
	{
		try
		{
			return new ReentrantReadWriteLock(true);
		}
		catch (NoSuchMethodError e)
		{
			return new ReentrantReadWriteLock();
		}
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#loadState()
	 */
	public String[] loadState() throws java.sql.SQLException
	{
		try
		{
			preferences.sync();
			
			String state = preferences.get(this.id, null);
			
			if (state == null)
			{
				return null;
			}
			
			if (state.length() == 0)
			{
				return new String[0];
			}
			
			String[] databases = state.split(STATE_DELIMITER);
			
			// Validate persisted cluster state
			for (String id: databases)
			{
				if (!this.databaseMap.containsKey(id))
				{
					// Persisted cluster state is invalid!
					preferences.remove(this.id);					
					preferences.flush();
					
					return null;
				}
			}
			
			return databases;
		}
		catch (BackingStoreException e)
		{
			throw new SQLException(Messages.getMessage(Messages.CLUSTER_STATE_LOAD_FAILED, this), e);
		}
	}

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getConnectionFactoryMap()
	 */
	public Map<Database, ?> getConnectionFactoryMap()
	{
		return this.connectionFactoryMap;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#isAlive(net.sf.hajdbc.Database)
	 */
	public boolean isAlive(Database database)
	{
		Connection connection = null;
		
		try
		{
			connection = database.connect(this.connectionFactoryMap.get(database));
			
			Statement statement = connection.createStatement();
			
			statement.execute(this.dialect.getSimpleSQL());

			statement.close();
			
			return true;
		}
		catch (java.sql.SQLException e)
		{
			return false;
		}
		finally
		{
			if (connection != null)
			{
				try
				{
					connection.close();
				}
				catch (java.sql.SQLException e)
				{
					// Ignore
				}
			}
		}
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#deactivate(net.sf.hajdbc.Database)
	 */
	public boolean deactivate(Database database)
	{
		boolean removed = this.balancer.remove(database);
		
		if (removed)
		{
			this.storeState();
		}
		
		return removed;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#getId()
	 */
	public String getId()
	{
		return this.id;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#activate(net.sf.hajdbc.Database)
	 */
	public boolean activate(Database database)
	{
		boolean added = this.balancer.add(database);
		
		if (added)
		{
			this.storeState();
		}
		
		return added;
	}
	
	private void storeState()
	{
		StringBuilder builder = new StringBuilder();
		
		Iterator<Database> databases = this.balancer.list().iterator();
		
		while (databases.hasNext())
		{
			builder.append(databases.next().getId());
			
			if (databases.hasNext())
			{
				builder.append(STATE_DELIMITER);
			}
		}
		
		preferences.put(this.id, builder.toString());
		
		try
		{
			preferences.flush();
		}
		catch (BackingStoreException e)
		{
			log.warn(Messages.getMessage(Messages.CLUSTER_STATE_STORE_FAILED, this), e);
		}
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#getActiveDatabases()
	 */
	public Collection<String> getActiveDatabases()
	{
		return this.getDatabaseIds(this.balancer.list());
	}

	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#getInactiveDatabases()
	 */
	public Collection<String> getInactiveDatabases()
	{
		Set<Database> databaseSet = new HashSet<Database>(this.databaseMap.values());

		databaseSet.removeAll(this.balancer.list());

		return this.getDatabaseIds(databaseSet);
	}
	
	private List<String> getDatabaseIds(Collection<Database> databases)
	{
		List<String> databaseList = new ArrayList<String>(databases.size());
		
		for (Database database: databases)
		{
			databaseList.add(database.getId());
		}
		
		return databaseList;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getDatabase(java.lang.String)
	 */
	public Database getDatabase(String id)
	{
		Database database = this.databaseMap.get(id);
		
		if (database == null)
		{
			throw new IllegalArgumentException(Messages.getMessage(Messages.INVALID_DATABASE, id, this));
		}
		
		return database;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getDefaultSynchronizationStrategy()
	 */
	public SynchronizationStrategy getDefaultSynchronizationStrategy()
	{
		return DatabaseClusterFactory.getInstance().getSynchronizationStrategy(this.defaultSynchronizationStrategyId);
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getBalancer()
	 */
	public Balancer getBalancer()
	{
		return this.balancer;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getExecutor()
	 */
	public ExecutorService getExecutor()
	{
		return this.executor;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getDialect()
	 */
	public Dialect getDialect()
	{
		return this.dialect;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#readLock()
	 */
	public Lock readLock()
	{
		return this.lock.readLock();
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#writeLock()
	 */
	public Lock writeLock()
	{
		return this.lock.writeLock();
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#isAlive(java.lang.String)
	 */
	public final boolean isAlive(String id)
	{
		return this.isAlive(this.getDatabase(id));
	}

	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#deactivate(java.lang.String)
	 */
	public final void deactivate(String databaseId)
	{
		if (this.deactivate(this.getDatabase(databaseId)))
		{
			log.info(Messages.getMessage(Messages.DATABASE_DEACTIVATED, databaseId, this));
		}
	}

	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#activate(java.lang.String)
	 */
	public final void activate(String databaseId) throws Exception
	{
		this.activate(databaseId, this.getDefaultSynchronizationStrategy());
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#activate(java.lang.String, java.lang.String)
	 */
	public final void activate(String databaseId, String strategyId) throws Exception
	{
		this.activate(databaseId, DatabaseClusterFactory.getInstance().getSynchronizationStrategy(strategyId));
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#getVersion()
	 */
	public String getVersion()
	{
		return DatabaseClusterFactory.getVersion();
	}

	/**
	 * Handles a failure caused by the specified cause on the specified database.
	 * If the database is not alive, then it is deactivated, otherwise an exception is thrown back to the caller.
	 * @param database a database descriptor
	 * @param cause the cause of the failure
	 * @throws java.sql.SQLException if the database is alive
	 */
	public final void handleFailure(Database database, java.sql.SQLException cause) throws java.sql.SQLException
	{
		if (this.isAlive(database))
		{
			throw cause;
		}
		
		log.warn(Messages.getMessage(Messages.DATABASE_NOT_ALIVE, database, this), cause);
		
		this.deactivate(database);
	}
	
	/**
	 * @see java.lang.Object#toString()
	 */
	public final String toString()
	{
		return this.getId();
	}
	
	/**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public final boolean equals(Object object)
	{
		DatabaseCluster databaseCluster = (DatabaseCluster) object;
		
		return this.getId().equals(databaseCluster.getId());
	}
	
	void setId(String id)
	{
		this.id = id;
	}

	void setDialect(Dialect dialect)
	{
		this.dialect = dialect;
	}
	
	void setBalancer(Balancer balancer)
	{
		this.balancer = balancer;
	}
	
	SynchronizationStrategyBuilder getDefaultSynchronizationStrategyBuilder()
	{
		return new SynchronizationStrategyBuilder(this.defaultSynchronizationStrategyId);
	}
	
	void setDefaultSynchronizationStrategyBuilder(SynchronizationStrategyBuilder builder)
	{
		this.defaultSynchronizationStrategyId = builder.getId();
	}

	void setExecutor(ThreadPoolExecutor executor)
	{
		this.executor = executor;
	}
	
	void addDatabase(Database database)
	{
		this.databaseMap.put(database.getId(), database);
		
		MBeanServer server = DatabaseClusterFactory.getMBeanServer();

		try
		{
			ObjectName name = DatabaseClusterFactory.getObjectName(this.id, database.getId());
			
			if (!server.isRegistered(name))
			{
				server.registerMBean(database, name);
			}
		}
		catch (Exception e)
		{
			log.error(e.getMessage(), e);
			throw new RuntimeException(e);
		}
	}
	
	Iterator<Database> getDriverDatabases()
	{
		return this.getDatabases(Driver.class);
	}
	
	Iterator<Database> getDataSourceDatabases()
	{
		return this.getDatabases(DataSource.class);
	}
	
	Iterator<Database> getDatabases(Class targetClass)
	{
		List<Database> databaseList = new ArrayList<Database>(this.databaseMap.size());
		
		for (Database database: this.databaseMap.values())
		{
			if (targetClass.equals(database.getConnectionFactoryClass()))
			{
				databaseList.add(database);
			}
		}
		
		return databaseList.iterator();
	}
	
	void setMinThreads(int threads)
	{
		this.executor.setCorePoolSize(threads);
	}
	
	int getMinThreads()
	{
		return this.executor.getCorePoolSize();
	}
	
	void setMaxThreads(int threads)
	{
		this.executor.setMaximumPoolSize(threads);
	}
	
	int getMaxThreads()
	{
		return this.executor.getMaximumPoolSize();
	}
	
	void setMaxIdle(int seconds)
	{
		this.executor.setKeepAliveTime(seconds, TimeUnit.SECONDS);
	}

	int getMaxIdle()
	{
		return Long.valueOf(this.executor.getKeepAliveTime(TimeUnit.SECONDS)).intValue();
	}
	
	void setFailureDetectionPeriod(int seconds)
	{
		this.failureDetectionPeriod = seconds;
	}
	
	int getFailureDetectionPeriod()
	{
		return this.failureDetectionPeriod;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#start()
	 */
	public void start() throws java.sql.SQLException
	{
		for (Database database: this.databaseMap.values())
		{
			this.connectionFactoryMap.put(database, database.createConnectionFactory());
		}
		
		String[] databases = this.loadState();

		if (databases != null)
		{
			for (String id: databases)
			{
				Database database = this.getDatabase(id);
				
				this.activate(database);
			}
		}
		else
		{
			for (String id: this.getInactiveDatabases())
			{
				Database database = this.getDatabase(id);
				
				if (this.isAlive(database))
				{
					this.activate(database);
				}
			}
		}
		
		if (this.failureDetectionPeriod > 0)
		{
			this.scheduledExecutor.scheduleWithFixedDelay(new FailureDetectionTask(), this.failureDetectionPeriod, this.failureDetectionPeriod, TimeUnit.SECONDS);
		}
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#stop()
	 */
	public void stop()
	{
		this.scheduledExecutor.shutdownNow();
		this.executor.shutdown();
	}
	
	private class FailureDetectionTask implements Runnable
	{
		/**
		 * @see java.lang.Runnable#run()
		 */
		public void run()
		{
			for (Database database: LocalDatabaseCluster.this.getBalancer().list())
			{
				if (!LocalDatabaseCluster.this.isAlive(database))
				{
					log.warn(Messages.getMessage(Messages.DATABASE_NOT_ALIVE, database, this));
					
					LocalDatabaseCluster.this.deactivate(database);
				}
			}
		}
	}	
	
	private void activate(String databaseId, SynchronizationStrategy strategy) throws Exception
	{
		try
		{
			if (this.activate(this.getDatabase(databaseId), strategy))
			{
				log.info(Messages.getMessage(Messages.DATABASE_ACTIVATED, databaseId, this));
			}
		}
		catch (java.sql.SQLException e)
		{
			java.sql.SQLException exception = e;
			
			while (exception != null)
			{
				log.error(exception.toString(), e);
				
				exception = exception.getNextException();
			}

			throw new Exception(Messages.getMessage(Messages.DATABASE_ACTIVATE_FAILED, databaseId, this));
		}
	}
	
	private boolean activate(Database database, SynchronizationStrategy strategy) throws java.sql.SQLException, InterruptedException
	{
		if (this.getBalancer().contains(database))
		{
			return false;
		}
		
		Lock lock = this.writeLock();
		
		lock.lockInterruptibly();
		
		try
		{
			List<Database> databaseList = this.getBalancer().list();
			
			if (databaseList.isEmpty())
			{
				return this.activate(database);
			}
			
			this.activate(database, databaseList, strategy);
			
			return true;
		}
		finally
		{
			lock.unlock();
		}
	}
	
	private void activate(Database inactiveDatabase, List<Database> activeDatabaseList, SynchronizationStrategy strategy) throws java.sql.SQLException
	{
		Database activeDatabase = this.getBalancer().next();
		
		Connection inactiveConnection = null;
		Connection activeConnection = null;

		List<Connection> connectionList = new ArrayList<Connection>(activeDatabaseList.size());
		
		try
		{
			Map<Database, ?> connectionFactoryMap = this.getConnectionFactoryMap();
			
			inactiveConnection = inactiveDatabase.connect(connectionFactoryMap.get(inactiveDatabase));
			
			Map<String, List<String>> schemaMap = new HashMap<String, List<String>>();
			
			DatabaseMetaData metaData = inactiveConnection.getMetaData();

			ResultSet resultSet = metaData.getTables(null, null, "%", new String[] { "TABLE" });
			
			while (resultSet.next())
			{
				String table = resultSet.getString("TABLE_NAME");
				String schema = resultSet.getString("TABLE_SCHEM");

				List<String> tableList = schemaMap.get(schema);
				
				if (tableList == null)
				{
					tableList = new LinkedList();
					
					schemaMap.put(schema, tableList);
				}
				
				tableList.add(table);
			}
			
			resultSet.close();

			activeConnection = activeDatabase.connect(connectionFactoryMap.get(activeDatabase));

			Dialect dialect = this.getDialect();
			
			if (strategy.requiresTableLocking())
			{
				log.info(Messages.getMessage(Messages.TABLE_LOCK_ACQUIRE));
				
				Map<String, Map<String, String>> lockTableSQLMap = new HashMap<String, Map<String, String>>();
				
				// Lock all tables on all active databases
				for (Database database: activeDatabaseList)
				{
					Connection connection = database.equals(activeDatabase) ? activeConnection : database.connect(connectionFactoryMap.get(database));
					
					connectionList.add(connection);
					
					connection.setAutoCommit(false);
					connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
					
					Statement statement = connection.createStatement();
					
					for (Map.Entry<String, List<String>> schemaMapEntry: schemaMap.entrySet())
					{
						String schema = schemaMapEntry.getKey();
						
						Map<String, String> map = lockTableSQLMap.get(schema);
						
						if (map == null)
						{
							map = new HashMap<String, String>();
							
							lockTableSQLMap.put(schema, map);
						}
						
						for (String table: schemaMapEntry.getValue())
						{
							String sql = map.get(table);
							
							if (sql == null)
							{
								sql = dialect.getLockTableSQL(metaData, schema, table);
								
								log.debug(sql);
								
								map.put(table, sql);
							}
							
							statement.addBatch(sql);
						}
						
						statement.executeBatch();
						statement.clearBatch();
					}
					
					statement.close();
				}
			}
			
			log.info(Messages.getMessage(Messages.DATABASE_SYNC_START, inactiveDatabase, this));

			strategy.synchronize(inactiveConnection, activeConnection, schemaMap, dialect);
			
			log.info(Messages.getMessage(Messages.DATABASE_SYNC_END, inactiveDatabase, this));
	
			this.activate(inactiveDatabase);
			
			if (strategy.requiresTableLocking())
			{
				log.info(Messages.getMessage(Messages.TABLE_LOCK_ACQUIRE));
				
				// Release table locks
				this.rollback(connectionList);
			}
		}
		catch (java.sql.SQLException e)
		{
			this.rollback(connectionList);
			
			throw e;
		}
		finally
		{
			this.close(activeConnection);
			this.close(inactiveConnection);
			
			for (Connection connection: connectionList)
			{
				this.close(connection);
			}
		}
	}
	
	private void rollback(List<Connection> connectionList)
	{
		for (Connection connection: connectionList)
		{
			try
			{
				connection.rollback();
				connection.setAutoCommit(true);
			}
			catch (java.sql.SQLException e)
			{
				log.warn(e.getMessage(), e);
			}
		}
	}
	
	private void close(Connection connection)
	{
		if (connection != null)
		{
			try
			{
				if (!connection.isClosed())
				{
					connection.close();
				}
			}
			catch (java.sql.SQLException e)
			{
				log.warn(e.getMessage(), e);
			}
		}
	}
}
