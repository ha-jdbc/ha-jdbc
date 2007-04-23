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
package net.sf.hajdbc.sql;

import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import javax.management.DynamicMBean;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseClusterFactory;
import net.sf.hajdbc.DatabaseClusterMBean;
import net.sf.hajdbc.DatabaseMetaDataCache;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.LockManager;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.StateManager;
import net.sf.hajdbc.SynchronizationContext;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.SynchronizationStrategyBuilder;
import net.sf.hajdbc.TransactionMode;
import net.sf.hajdbc.local.LocalLockManager;
import net.sf.hajdbc.local.LocalStateManager;
import net.sf.hajdbc.sync.SynchronizationContextImpl;
import net.sf.hajdbc.util.concurrent.CronThreadPoolExecutor;
import net.sf.hajdbc.util.concurrent.DaemonThreadFactory;
import net.sf.hajdbc.util.concurrent.SynchronousExecutor;

import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author  Paul Ferraro
 * @param <D> either java.sql.Driver or javax.sql.DataSource
 * @since   1.0
 */
public abstract class AbstractDatabaseCluster<D> implements DatabaseCluster<D>, DatabaseClusterMBean
{
	private static final String MBEAN_DOMAIN = "net.sf.hajdbc";
	private static final String MBEAN_CLUSTER_KEY = "cluster";
	private static final String MBEAN_DATABASE_KEY = "database";
		
	static Logger logger = LoggerFactory.getLogger(AbstractDatabaseCluster.class);
	
	/**
	 * Convenience method for constructing a standardized mbean ObjectName for this cluster.
	 * @param databaseClusterId a cluster identifier
	 * @return an ObjectName for this cluster
	 * @throws MalformedObjectNameException if the ObjectName could not be constructed
	 */
	public static ObjectName getObjectName(String databaseClusterId) throws MalformedObjectNameException
	{
		return ObjectName.getInstance(MBEAN_DOMAIN, createProperties(databaseClusterId));
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
		Hashtable<String, String> properties = createProperties(databaseClusterId);
		
		properties.put(MBEAN_DATABASE_KEY, databaseId);
		
		return ObjectName.getInstance(MBEAN_DOMAIN, properties);
	}
	
	private static Hashtable<String, String> createProperties(String databaseClusterId)
	{
		Hashtable<String, String> properties = new Hashtable<String, String>();
		
		properties.put(MBEAN_CLUSTER_KEY, databaseClusterId);
		
		return properties;
	}
	
	private String id;
	private Balancer<D> balancer;
	private Dialect dialect;
	private DatabaseMetaDataCache databaseMetaDataCache;
	private String defaultSynchronizationStrategyId;
	private CronExpression failureDetectionExpression;
	private CronExpression autoActivationExpression;
	private int minThreads;
	private int maxThreads;
	private int maxIdle;
	private TransactionMode transactionMode;
	private boolean identityColumnDetectionEnabled;
	private boolean sequenceDetectionEnabled;
	
	private MBeanServer server;
	private Map<String, Database<D>> databaseMap = new ConcurrentHashMap<String, Database<D>>();
	private Map<Database<D>, D> connectionFactoryMap = new ConcurrentHashMap<Database<D>, D>();
	private ExecutorService transactionalExecutor;
	private ExecutorService nonTransactionalExecutor;
	private CronThreadPoolExecutor cronExecutor = new CronThreadPoolExecutor(2);
	private LockManager lockManager = new LocalLockManager();
	private StateManager stateManager = new LocalStateManager(this);
	
	public AbstractDatabaseCluster()
	{
		this.server = ManagementFactory.getPlatformMBeanServer();
	}

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getConnectionFactoryMap()
	 */
	public Map<Database<D>, D> getConnectionFactoryMap()
	{
		return this.connectionFactoryMap;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#isAlive(net.sf.hajdbc.Database)
	 */
	public boolean isAlive(Database<D> database)
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
	public synchronized boolean deactivate(Database<D> database)
	{
		this.unregister(database);
		// Reregister database mbean using "inactive" interface
		this.register(database, database.getInactiveMBean());
		
		boolean removed = this.balancer.remove(database);
		
		if (removed)
		{
			this.stateManager.remove(database.getId());
		}
		
		return removed;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getId()
	 */
	public String getId()
	{
		return this.id;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#activate(net.sf.hajdbc.Database)
	 */
	public synchronized boolean activate(Database<D> database)
	{
		this.unregister(database);
		// Reregister database mbean using "active" interface
		this.register(database, database.getActiveMBean());
		
		if (database.isDirty())
		{
			DatabaseClusterFactory.getInstance().exportConfig();
			
			database.clean();
		}
		
		boolean added = this.balancer.add(database);
		
		if (added)
		{
			this.stateManager.add(database.getId());
		}
		
		return added;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#getActiveDatabases()
	 */
	public Set<String> getActiveDatabases()
	{
		Set<String> databaseSet = new TreeSet<String>();
		
		for (Database<D> database: this.balancer.all())
		{
			databaseSet.add(database.getId());
		}
		
		return databaseSet;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#getInactiveDatabases()
	 */
	public Set<String> getInactiveDatabases()
	{
		Set<String> databaseSet = new TreeSet<String>(this.databaseMap.keySet());

		for (Database<D> database: this.balancer.all())
		{
			databaseSet.remove(database.getId());
		}
		
		return databaseSet;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getDatabase(java.lang.String)
	 */
	public Database<D> getDatabase(String id)
	{
		Database<D> database = this.databaseMap.get(id);
		
		if (database == null)
		{
			throw new IllegalArgumentException(Messages.getMessage(Messages.INVALID_DATABASE, id, this));
		}
		
		return database;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#getDefaultSynchronizationStrategy()
	 */
	public String getDefaultSynchronizationStrategy()
	{
		return this.defaultSynchronizationStrategyId;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#getSynchronizationStrategies()
	 */
	public Set<String> getSynchronizationStrategies()
	{
		return new TreeSet<String>(DatabaseClusterFactory.getInstance().getSynchronizationStrategyMap().keySet());
	}

	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#getVersion()
	 */
	public String getVersion()
	{
		return DatabaseClusterFactory.getVersion();
	}

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getBalancer()
	 */
	public Balancer<D> getBalancer()
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
		SynchronizationStrategy strategy = DatabaseClusterFactory.getInstance().getSynchronizationStrategyMap().get(strategyId);
		
		if (strategy == null)
		{
			throw new IllegalArgumentException(Messages.getMessage(Messages.INVALID_SYNC_STRATEGY, strategyId));
		}
		
		this.activate(databaseId, strategy);
	}

	/**
	 * Handles a failure caused by the specified cause on the specified database.
	 * If the database is not alive, then it is deactivated, otherwise an exception is thrown back to the caller.
	 * @param database a database descriptor
	 * @param cause the cause of the failure
	 * @throws java.sql.SQLException if the database is alive
	 */
	public final void handleFailure(Database<D> database, java.sql.SQLException cause) throws java.sql.SQLException
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
	
	protected void register(Database<D> database, DynamicMBean mbean)
	{
		try
		{
			ObjectName name = getObjectName(this.id, database.getId());
			
			this.server.registerMBean(mbean, name);
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
		Database<D> database = this.getDatabase(id);
		
		if (this.balancer.contains(database))
		{
			throw new IllegalStateException(Messages.getMessage(Messages.DATABASE_STILL_ACTIVE, id, this));
		}

		this.unregister(database);
		
		this.databaseMap.remove(id);
		this.connectionFactoryMap.remove(database);
		
		DatabaseClusterFactory.getInstance().exportConfig();
	}

	private void unregister(Database<D> database)
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
	public void start() throws Exception
	{
		this.server.registerMBean(this.createMBean(), getObjectName(this.id));
		
		for (Database<D> database: this.databaseMap.values())
		{
			this.register(database, database.getInactiveMBean());
		}
		
		this.lockManager.start();
		this.stateManager.start();
		
		Set<String> databaseSet = this.stateManager.getInitialState();
		
		if (databaseSet != null)
		{
			for (String databaseId: databaseSet)
			{
				Database<D> database = this.getDatabase(databaseId);
				
				if (database != null)
				{
					this.activate(database);
				}
			}
		}
		else
		{
			for (Database<D> database: this.databaseMap.values())
			{
				if (this.isAlive(database))
				{
					this.activate(database);
				}
			}
		}
		
		this.nonTransactionalExecutor = new ThreadPoolExecutor(this.minThreads, this.maxThreads, this.maxIdle, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), DaemonThreadFactory.getInstance(), new ThreadPoolExecutor.CallerRunsPolicy());
		
		this.transactionalExecutor = this.transactionMode.equals(TransactionMode.SERIAL) ? new SynchronousExecutor() : this.nonTransactionalExecutor;
		
		this.databaseMetaDataCache.setDialect(this.dialect);
		
		try
		{
			this.flushMetaDataCache();
		}
		catch (IllegalStateException e)
		{
			// Ignore - cache will initialize lazily.
		}
		
		if (this.failureDetectionExpression != null)
		{
			this.cronExecutor.schedule(new FailureDetectionTask(), this.failureDetectionExpression);
		}
		
		if (this.autoActivationExpression != null)
		{
			this.cronExecutor.schedule(new AutoActivationTask(), this.autoActivationExpression);
		}
	}
	
	protected abstract DynamicMBean createMBean() throws NotCompliantMBeanException;

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#stop()
	 */
	public synchronized void stop()
	{
		for (Database<D> database: this.databaseMap.values())
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
		
		this.stateManager.stop();
		this.lockManager.stop();
		
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
		
		try
		{
			Database database = this.balancer.next();
			
			connection = database.connect(this.connectionFactoryMap.get(database));
			
			this.databaseMetaDataCache.flush(connection);
		}
		catch (NoSuchElementException e)
		{
			throw new IllegalStateException(Messages.getMessage(Messages.NO_ACTIVE_DATABASES, this));
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
	
	protected synchronized void add(Database<D> database)
	{
		String id = database.getId();
		
		if (this.databaseMap.containsKey(id))
		{
			throw new IllegalArgumentException(Messages.getMessage(Messages.DATABASE_ALREADY_EXISTS, id, this));
		}
		
		this.connectionFactoryMap.put(database, database.createConnectionFactory());
		this.databaseMap.put(id, database);
	}
	
	Iterator<Database<D>> getDatabases()
	{
		return this.databaseMap.values().iterator();
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getStateManager()
	 */
	public StateManager getStateManager()
	{
		return this.stateManager;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#setStateManager(net.sf.hajdbc.StateManager)
	 */
	public void setStateManager(StateManager stateManager)
	{
		this.stateManager = stateManager;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#setLockManager(net.sf.hajdbc.LockManager)
	 */
	public void setLockManager(LockManager lockManager)
	{
		this.lockManager = lockManager;
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
	
	private boolean activate(Database<D> database, SynchronizationStrategy strategy) throws java.sql.SQLException, InterruptedException
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
			SynchronizationContext<D> context = new SynchronizationContextImpl<D>(this, database);
			
			try
			{
				if (!context.getActiveDatabaseSet().isEmpty())
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
			for (String databaseId: AbstractDatabaseCluster.this.getActiveDatabases())
			{
				if (!AbstractDatabaseCluster.this.isAlive(databaseId))
				{
					AbstractDatabaseCluster.this.deactivate(databaseId);
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
			for (String databaseId: AbstractDatabaseCluster.this.getInactiveDatabases())
			{
				AbstractDatabaseCluster.this.activate(databaseId);
			}
		}
	}
}
