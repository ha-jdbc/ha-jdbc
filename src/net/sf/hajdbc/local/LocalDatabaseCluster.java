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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseClusterFactory;
import net.sf.hajdbc.DatabaseClusterMBean;
import net.sf.hajdbc.DatabaseMetaDataCache;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.LockManager;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.SQLException;
import net.sf.hajdbc.SynchronizationContext;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.SynchronizationStrategyBuilder;
import net.sf.hajdbc.sql.DataSourceDatabase;
import net.sf.hajdbc.sql.DriverDatabase;
import net.sf.hajdbc.sync.SynchronizationContextImpl;
import net.sf.hajdbc.util.concurrent.CronThreadPoolExecutor;
import net.sf.hajdbc.util.concurrent.SynchronousExecutor;

import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class LocalDatabaseCluster implements DatabaseCluster
{
	private static final String JMX_AGENT_PROPERTY = "ha-jdbc.jmx-agent";
	private static final String STATE_DELIMITER = ",";
	private static final String MBEAN_DOMAIN = "net.sf.hajdbc";
	private static final String MBEAN_CLUSTER_KEY = "cluster";
	private static final String MBEAN_DATABASE_KEY = "database";
		
	private static Preferences preferences = Preferences.userNodeForPackage(LocalDatabaseCluster.class);
	static Logger logger = LoggerFactory.getLogger(LocalDatabaseCluster.class);
	
	/**
	 * Convenience method for constructing a standardized mbean ObjectName for this cluster.
	 * @param databaseClusterId a cluster identifier
	 * @return an ObjectName for this cluster
	 * @throws MalformedObjectNameException if the ObjectName could not be constructed
	 */
	public static ObjectName getObjectName(String databaseClusterId) throws MalformedObjectNameException
	{
		return getObjectName(databaseClusterId, new Properties());
	}

	/**
	 * Convenience method for constructing a standardized mbean ObjectName for this database.
	 * @param databaseClusterId a cluster identifier
	 * @param databaseId a database identifier
	 * @return an ObjectName for this cluster
	 * @throws MalformedObjectNameException if the ObjectName could not be constructed
	 */
	public static ObjectName getObjectName(String databaseClusterId, String databaseId) throws MalformedObjectNameException
	{
		Properties properties = new Properties();
		properties.setProperty(MBEAN_DATABASE_KEY, ObjectName.quote(databaseId));
		
		return getObjectName(databaseClusterId, properties);
	}
	
	private static ObjectName getObjectName(String databaseClusterId, Properties properties) throws MalformedObjectNameException
	{
		properties.setProperty(MBEAN_CLUSTER_KEY, ObjectName.quote(databaseClusterId));
		
		return ObjectName.getInstance(MBEAN_DOMAIN, properties);
	}
	
	private String id;
	private Balancer balancer;
	private Dialect dialect;
	private DatabaseMetaDataCache databaseMetaDataCache;
	private String defaultSynchronizationStrategyId;
	private CronExpression failureDetectionExpression;
	private CronExpression autoActivationExpression;
	private int minThreads;
	private int maxThreads;
	private int maxIdle;
	private Transaction transaction;
	private boolean identityColumnDetectionEnabled;
	private boolean sequenceDetectionEnabled;
	
	private MBeanServer server;
	private Map<String, Database> databaseMap = new ConcurrentHashMap<String, Database>();
	private Map<Database, Object> connectionFactoryMap = new ConcurrentHashMap<Database, Object>();
	private ExecutorService transactionalExecutor;
	private ExecutorService nonTransactionalExecutor;
	private CronThreadPoolExecutor cronExecutor = new CronThreadPoolExecutor(2);
	private LockManager lockManager = new LocalLockManager();
	
	public LocalDatabaseCluster()
	{
		String agent = System.getProperty(JMX_AGENT_PROPERTY);
		
		List serverList = MBeanServerFactory.findMBeanServer(agent);
		
		if (serverList.isEmpty())
		{
			throw new IllegalStateException(Messages.getMessage(Messages.MBEAN_SERVER_NOT_FOUND));
		}
		
		this.server = MBeanServer.class.cast(serverList.get(0));
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
	@SuppressWarnings("unchecked")
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
			logger.info(Messages.getMessage(Messages.DATABASE_NOT_ALIVE, database, this), e);
			
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
					logger.warn(e.toString(), e);
				}
			}
		}
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#deactivate(net.sf.hajdbc.Database)
	 */
	public synchronized boolean deactivate(Database database)
	{
		this.unregister(database);
		// Reregister database mbean using "inactive" interface
		this.register(database, database.getInactiveMBeanClass());
		
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
	public synchronized boolean activate(Database database)
	{
		this.unregister(database);
		// Reregister database mbean using "active" interface
		this.register(database, database.getActiveMBeanClass());
		
		if (database.isDirty())
		{
			DatabaseClusterFactory.getInstance().exportConfiguration();
			
			database.clean();
		}
		
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
			logger.warn(Messages.getMessage(Messages.CLUSTER_STATE_STORE_FAILED, this), e);
		}
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#getActiveDatabases()
	 */
	public Collection<String> getActiveDatabases()
	{
		return this.extractIdentifiers(this.balancer.list());
	}

	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#getInactiveDatabases()
	 */
	public Collection<String> getInactiveDatabases()
	{
		return this.extractIdentifiers(this.getInactiveDatabaseSet());
	}
	
	protected Set<Database> getInactiveDatabaseSet()
	{
		Set<Database> databaseSet = new TreeSet<Database>(this.databaseMap.values());

		databaseSet.removeAll(this.balancer.list());
		
		return databaseSet;
	}
	
	private List<String> extractIdentifiers(Collection<Database> databases)
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
	 * @see net.sf.hajdbc.DatabaseCluster#getTransactionalExecutor()
	 */
	public ExecutorService getTransactionalExecutor()
	{
		return this.transactionalExecutor;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getNonTransactionalExecutor()
	 */
	public ExecutorService getNonTransactionalExecutor()
	{
		return this.nonTransactionalExecutor;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getDialect()
	 */
	public Dialect getDialect()
	{
		return this.dialect;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getDatabaseMetaDataCache()
	 */
	public DatabaseMetaDataCache getDatabaseMetaDataCache()
	{
		return this.databaseMetaDataCache;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getLockManager()
	 */
	public LockManager getLockManager()
	{
		return this.lockManager;
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
			logger.info(Messages.getMessage(Messages.DATABASE_DEACTIVATED, databaseId, this));
		}
	}

	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#activate(java.lang.String)
	 */
	public final void activate(String databaseId)
	{
		this.activate(databaseId, this.getDefaultSynchronizationStrategy());
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#activate(java.lang.String, java.lang.String)
	 */
	public final void activate(String databaseId, String strategyId)
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
		
		if (this.deactivate(database))
		{
			logger.warn(Messages.getMessage(Messages.DATABASE_NOT_ALIVE, database, this), cause);
		}
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#add(java.lang.String, java.lang.String, java.lang.String)
	 */
	public void add(String id, String driver, String url)
	{
		DriverDatabase database = new DriverDatabase();
		
		database.setId(id);
		database.setDriver(driver);
		database.setUrl(url);
		
		this.register(database, database.getInactiveMBeanClass());
		
		this.add(database);
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#add(java.lang.String, java.lang.String)
	 */
	public void add(String id, String name)
	{
		DataSourceDatabase database = new DataSourceDatabase();
		
		database.setId(id);
		database.setName(name);
		
		this.register(database, database.getInactiveMBeanClass());
		
		this.add(database);
	}
	
	private void register(Database database, Class mbeanClass)
	{
		try
		{
			ObjectName name = getObjectName(this.id, database.getId());
			
			this.server.registerMBean(new StandardMBean(database, mbeanClass), name);
		}
		catch (JMException e)
		{
			logger.error(e.toString(), e);
			
			throw new IllegalStateException(e);
		}
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#remove(java.lang.String)
	 */
	public synchronized void remove(String id)
	{
		Database database = this.getDatabase(id);
		
		if (this.balancer.contains(database))
		{
			throw new IllegalStateException(Messages.getMessage(Messages.DATABASE_STILL_ACTIVE, id, this));
		}

		this.unregister(database);
		
		this.databaseMap.remove(id);
		this.connectionFactoryMap.remove(database);
		
		DatabaseClusterFactory.getInstance().exportConfiguration();
	}

	private void unregister(Database database)
	{
		try
		{
			ObjectName name = getObjectName(this.id, database.getId());
			
			if (this.server.isRegistered(name))
			{
				this.server.unregisterMBean(name);
			}
		}
		catch (JMException e)
		{
			logger.error(e.toString(), e);
			
			throw new IllegalStateException(e);
		}
	}

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#start()
	 */
	public void start() throws java.sql.SQLException
	{
		try
		{
			this.server.registerMBean(new StandardMBean(this, DatabaseClusterMBean.class), getObjectName(this.id));
			
			for (Database database: this.databaseMap.values())
			{
				this.register(database, database.getInactiveMBeanClass());
			}
		}
		catch (JMException e)
		{
			throw new SQLException(e);
		}
		
		String[] databases = this.loadState();
		
		if (databases != null)
		{
			for (String id: databases)
			{
				this.activate(this.getDatabase(id));
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
		
		this.nonTransactionalExecutor = new ThreadPoolExecutor(this.minThreads, this.maxThreads, this.maxIdle, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), new ThreadPoolExecutor.CallerRunsPolicy());
		
		this.transactionalExecutor = this.transaction.equals(Transaction.XA) ? new SynchronousExecutor() : this.nonTransactionalExecutor;
		
		this.databaseMetaDataCache.setDialect(this.dialect);
		
		this.flushMetaDataCache();
		
		if (this.failureDetectionExpression != null)
		{
			this.cronExecutor.schedule(new FailureDetectionTask(), this.failureDetectionExpression);
		}
		
		if (this.autoActivationExpression != null)
		{
			this.cronExecutor.schedule(new AutoActivationTask(), this.autoActivationExpression);
		}
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#stop()
	 */
	public synchronized void stop()
	{
		for (Database database: this.databaseMap.values())
		{
			this.unregister(database);
		}
		
		try
		{
			ObjectName name = getObjectName(this.id);
			
			if (this.server.isRegistered(name))
			{
				this.server.unregisterMBean(name);
			}
		}
		catch (JMException e)
		{
			logger.warn(e.getMessage(), e);
		}
		
		this.cronExecutor.shutdownNow();
		
		if (this.nonTransactionalExecutor != null)
		{
			this.nonTransactionalExecutor.shutdownNow();
		}
		
		if (this.transactionalExecutor != null)
		{
			this.transactionalExecutor.shutdownNow();
		}
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#flushMetaDataCache()
	 */
	@SuppressWarnings("unchecked")
	public void flushMetaDataCache()
	{
		Connection connection = null;
		
		Database database = this.balancer.next();
		
		try
		{
			connection = database.connect(this.connectionFactoryMap.get(database));
			
			this.databaseMetaDataCache.flush(connection);
		}
		catch (java.sql.SQLException e)
		{
			throw new IllegalStateException(e.toString(), e);
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
					logger.warn(e.toString(), e);
				}
			}
		}
	}

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#isIdentityColumnDetectionEnabled()
	 */
	public boolean isIdentityColumnDetectionEnabled()
	{
		return this.identityColumnDetectionEnabled;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#isSequenceDetectionEnabled()
	 */
	public boolean isSequenceDetectionEnabled()
	{
		return this.sequenceDetectionEnabled;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public final String toString()
	{
		return this.getId();
	}
	
	/**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public final boolean equals(Object object)
	{
		return (object != null) && DatabaseCluster.class.isInstance(object) && this.id.equals(DatabaseCluster.class.cast(object).getId());
	}
	
	/**
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public final int hashCode()
	{
		return this.id.hashCode();
	}

	SynchronizationStrategyBuilder getDefaultSynchronizationStrategyBuilder()
	{
		return new SynchronizationStrategyBuilder(this.defaultSynchronizationStrategyId);
	}
	
	void setDefaultSynchronizationStrategyBuilder(SynchronizationStrategyBuilder builder)
	{
		this.defaultSynchronizationStrategyId = builder.getId();
	}
	
	synchronized void add(Database database)
	{
		String id = database.getId();
		
		if (this.databaseMap.containsKey(id))
		{
			throw new IllegalArgumentException(Messages.getMessage(Messages.DATABASE_ALREADY_EXISTS, id, this));
		}
		
		this.connectionFactoryMap.put(database, database.createConnectionFactory());
		this.databaseMap.put(id, database);
	}
	
	Iterator<Database> getDriverDatabases()
	{
		return this.getDatabases(DriverDatabase.class);
	}
	
	Iterator<Database> getDataSourceDatabases()
	{
		return this.getDatabases(DataSourceDatabase.class);
	}
	
	private Iterator<Database> getDatabases(Class targetClass)
	{
		List<Database> databaseList = new ArrayList<Database>(this.databaseMap.size());
		
		for (Database database: this.databaseMap.values())
		{
			if (targetClass.equals(database.getClass()))
			{
				databaseList.add(database);
			}
		}
		
		return databaseList.iterator();
	}
	
	private void activate(String databaseId, SynchronizationStrategy strategy)
	{
		try
		{
			if (this.activate(this.getDatabase(databaseId), strategy))
			{
				logger.info(Messages.getMessage(Messages.DATABASE_ACTIVATED, databaseId, this));
			}
		}
		catch (java.sql.SQLException e)
		{
			logger.error(Messages.getMessage(Messages.DATABASE_ACTIVATE_FAILED, databaseId, this), e);
			
			java.sql.SQLException exception = e.getNextException();
			
			while (exception != null)
			{
				logger.error(exception.getMessage(), e);
				
				exception = exception.getNextException();
			}

			throw new IllegalStateException(e.toString());
		}
		catch (InterruptedException e)
		{
			logger.warn(e.toString(), e);
			throw new IllegalMonitorStateException(e.toString());
		}
	}
	
	boolean activate(Database database, SynchronizationStrategy strategy) throws java.sql.SQLException, InterruptedException
	{
		if (this.getBalancer().contains(database))
		{
			return false;
		}
		
		if (!this.isAlive(database))
		{
			return false;
		}
		
		Lock lock = this.lockManager.writeLock(LockManager.GLOBAL);
		
		lock.lockInterruptibly();
		
		try
		{
			SynchronizationContext context = new SynchronizationContextImpl(this, database);
			
			try
			{
				if (!context.getActiveDatabases().isEmpty())
				{
					strategy.prepare(context);
					
					logger.info(Messages.getMessage(Messages.DATABASE_SYNC_START, database, this));
					
					strategy.synchronize(context);
					
					logger.info(Messages.getMessage(Messages.DATABASE_SYNC_END, database, this));
				}
				
				boolean activated = this.activate(database);
				
				strategy.cleanup(context);
				
				return activated;
			}
			finally
			{
				context.close();
			}
		}
		finally
		{
			lock.unlock();
		}
	}

	class FailureDetectionTask implements Runnable
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
					if (LocalDatabaseCluster.this.deactivate(database))
					{
						logger.warn(Messages.getMessage(Messages.DATABASE_NOT_ALIVE, database, this));
					}
				}
			}
		}
	}	
	
	class AutoActivationTask implements Runnable
	{
		/**
		 * @see java.lang.Runnable#run()
		 */
		public void run()
		{
			for (Database database: LocalDatabaseCluster.this.getInactiveDatabaseSet())
			{
				try
				{
					if (LocalDatabaseCluster.this.activate(database, LocalDatabaseCluster.this.getDefaultSynchronizationStrategy()))
					{
						logger.info(Messages.getMessage(Messages.DATABASE_ACTIVATED, database.getId(), this));
					}
				}
				catch (java.sql.SQLException e)
				{
					logger.warn(Messages.getMessage(Messages.DATABASE_ACTIVATE_FAILED, database, LocalDatabaseCluster.this), e);

					java.sql.SQLException exception = e.getNextException();
					
					while (exception != null)
					{
						logger.warn(exception.getMessage(), e);
						
						exception = exception.getNextException();
					}
				}
				catch (InterruptedException e)
				{
					break;
				}
			}
		}
	}
}
