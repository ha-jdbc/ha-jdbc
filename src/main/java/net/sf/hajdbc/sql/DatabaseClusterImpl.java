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
package net.sf.hajdbc.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseClusterConfiguration;
import net.sf.hajdbc.DatabaseClusterConfigurationListener;
import net.sf.hajdbc.DatabaseClusterListener;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.SynchronizationListener;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.TransactionMode;
import net.sf.hajdbc.Version;
import net.sf.hajdbc.balancer.Balancer;
import net.sf.hajdbc.cache.DatabaseMetaDataCache;
import net.sf.hajdbc.codec.Decoder;
import net.sf.hajdbc.dialect.Dialect;
import net.sf.hajdbc.distributed.CommandDispatcherFactory;
import net.sf.hajdbc.durability.Durability;
import net.sf.hajdbc.lock.LockManager;
import net.sf.hajdbc.lock.distributed.DistributedLockManager;
import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;
import net.sf.hajdbc.management.Description;
import net.sf.hajdbc.management.MBean;
import net.sf.hajdbc.management.MBeanRegistrar;
import net.sf.hajdbc.management.ManagedAttribute;
import net.sf.hajdbc.management.ManagedOperation;
import net.sf.hajdbc.state.DatabaseEvent;
import net.sf.hajdbc.state.StateManager;
import net.sf.hajdbc.state.distributed.DistributedStateManager;
import net.sf.hajdbc.sync.SynchronizationContext;
import net.sf.hajdbc.sync.SynchronizationContextImpl;
import net.sf.hajdbc.tx.TransactionIdentifierFactory;
import net.sf.hajdbc.util.Resources;
import net.sf.hajdbc.util.concurrent.cron.CronExpression;
import net.sf.hajdbc.util.concurrent.cron.CronThreadPoolExecutor;

/**
 * @author paul
 *
 */
@MBean
public class DatabaseClusterImpl<Z, D extends Database<Z>> implements DatabaseCluster<Z, D>
{
	static final Logger logger = LoggerFactory.getLogger(DatabaseClusterImpl.class);
	
	private final String id;
	
	final DatabaseClusterConfiguration<Z, D> configuration;
	
	private Balancer<Z, D> balancer;
	private Dialect dialect;
	private Durability<Z, D> durability;
	private DatabaseMetaDataCache<Z, D> databaseMetaDataCache;
	private ExecutorService executor;
	private Decoder decoder;
	private CronThreadPoolExecutor cronExecutor;
	private LockManager lockManager;
	private StateManager stateManager;
	
	private boolean active = false;
	
	private final List<DatabaseClusterConfigurationListener<Z, D>> configurationListeners = new CopyOnWriteArrayList<DatabaseClusterConfigurationListener<Z, D>>();	
	private final List<DatabaseClusterListener> clusterListeners = new CopyOnWriteArrayList<DatabaseClusterListener>();
	private final List<SynchronizationListener> synchronizationListeners = new CopyOnWriteArrayList<SynchronizationListener>();
	
	public DatabaseClusterImpl(String id, DatabaseClusterConfiguration<Z, D> configuration, DatabaseClusterConfigurationListener<Z, D> listener)
	{
		this.id = id;
		this.configuration = configuration;
		
		if (listener != null)
		{
			this.configurationListeners.add(listener);
		}
	}

	/**
	 * Deactivates the specified database.
	 * @param databaseId a database identifier
	 * @throws IllegalArgumentException if no database exists with the specified identifier.
	 */
	@ManagedOperation
	public void deactivate(String databaseId)
	{
		this.deactivate(this.getDatabase(databaseId), this.stateManager);
	}

	/**
	 * Synchronizes, using the default strategy, and reactivates the specified database.
	 * @param databaseId a database identifier
	 * @throws IllegalArgumentException if no database exists with the specified identifier.
	 * @throws IllegalStateException if synchronization fails.
	 */
	@ManagedOperation
	public void activate(String databaseId)
	{
		this.activate(databaseId, this.configuration.getDefaultSynchronizationStrategy());
	}

	/**
	 * Synchronizes, using the specified strategy, and reactivates the specified database.
	 * @param databaseId a database identifier
	 * @param strategyId the identifer of a synchronization strategy
	 * @throws IllegalArgumentException if no database exists with the specified identifier, or no synchronization strategy exists with the specified identifier.
	 * @throws IllegalStateException if synchronization fails.
	 */
	@ManagedOperation
	public void activate(String databaseId, String strategyId)
	{
		SynchronizationStrategy strategy = this.configuration.getSynchronizationStrategyMap().get(strategyId);
		
		if (strategy == null)
		{
			throw new IllegalArgumentException(Messages.INVALID_SYNC_STRATEGY.getMessage(this, strategyId));
		}
		
		try
		{
			if (this.activate(this.getDatabase(databaseId), strategy))
			{
				logger.log(Level.INFO, Messages.DATABASE_ACTIVATED.getMessage(this, databaseId));
			}
		}
		catch (SQLException e)
		{
			logger.log(Level.WARN, e, Messages.DATABASE_ACTIVATE_FAILED.getMessage(this, databaseId));
			
			SQLException exception = e.getNextException();
			
			while (exception != null)
			{
				logger.log(Level.ERROR, exception);
				
				exception = exception.getNextException();
			}

			throw new IllegalStateException(e.toString());
		}
		catch (InterruptedException e)
		{
			logger.log(Level.WARN, e);
			
			Thread.currentThread().interrupt();
		}
	}
	
	/**
	 * Determines whether or not the specified database is responsive
	 * @param databaseId a database identifier
	 * @return true, if the database is alive, false otherwise
	 * @throws IllegalArgumentException if no database exists with the specified identifier.
	 */
	@ManagedOperation
	public boolean isAlive(String databaseId)
	{
		return this.isAlive(this.getDatabase(databaseId));
	}
	
	/**
	 * Returns a collection of active databases in this cluster.
	 * @return a list of database identifiers
	 */
	@ManagedAttribute
	public Set<String> getActiveDatabases()
	{
		Set<String> databases = new TreeSet<String>();
		
		for (D database: this.balancer)
		{
			databases.add(database.getId());
		}
		
		return databases;
	}
	
	/**
	 * Returns a collection of inactive databases in this cluster.
	 * @return a collection of database identifiers
	 */
	@ManagedAttribute
	public Set<String> getInactiveDatabases()
	{
		Set<String> databases = new TreeSet<String>(this.configuration.getDatabaseMap().keySet());
		
		for (D database: this.balancer)
		{
			databases.remove(database.getId());
		}
		
		return databases;
	}
	
	/**
	 * Return the current HA-JDBC version
	 * @return the current version
	 */
	@ManagedAttribute
	public String getVersion()
	{
		return Version.getVersion();
	}
	
	/**
	 * Removes the specified database from the cluster.
	 * @param databaseId a database identifier
	 * @throws IllegalStateException if database is still active, or if mbean unregistration fails.
	 */
	@ManagedOperation
	public void remove(String databaseId)
	{
		D database = this.getDatabase(databaseId);
		
		if (this.balancer.contains(database))
		{
			throw new IllegalStateException(Messages.DATABASE_STILL_ACTIVE.getMessage(this, databaseId));
		}

		this.configuration.getMBeanRegistrar().unregister(this, database);
		
		this.configuration.getDatabaseMap().remove(databaseId);

		for (DatabaseClusterConfigurationListener<Z, D> listener: this.configurationListeners)
		{
			listener.removed(database, this.configuration);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseCluster#getId()
	 */
	@ManagedAttribute
	@Override
	public String getId()
	{
		return this.id;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseCluster#isActive()
	 */
	@ManagedAttribute
	@Override
	public boolean isActive()
	{
		return this.active;
	}
	
	/**
	 * Returns the set of synchronization strategies available to this cluster.
	 * @return a set of synchronization strategy identifiers
	 */
	@ManagedAttribute
	public Set<String> getSynchronizationStrategies()
	{
		return this.configuration.getSynchronizationStrategyMap().keySet();
	}
	
	/**
	 * Returns the default synchronization strategy used by this cluster.
	 * @return a synchronization strategy identifier
	 */
	@ManagedAttribute
	public String getDefaultSynchronizationStrategy()
	{
		return this.configuration.getDefaultSynchronizationStrategy();
	}

	/**
	 * Flushes this cluster's cache of DatabaseMetaData.
	 */
	@ManagedOperation
	@Description("Flushes this cluster's cache of database meta data")
	public void flushMetaDataCache()
	{
		try
		{
			this.databaseMetaDataCache.flush();
		}
		catch (SQLException e)
		{
			throw new IllegalStateException(e.toString(), e);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseCluster#addConfigurationListener(net.sf.hajdbc.DatabaseClusterConfigurationListener)
	 */
	@ManagedOperation
	@Override
	public void addConfigurationListener(DatabaseClusterConfigurationListener<Z, D> listener)
	{
		this.configurationListeners.add(listener);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseCluster#addListener(net.sf.hajdbc.DatabaseClusterListener)
	 */
	@ManagedOperation
	@Override
	public void addListener(DatabaseClusterListener listener)
	{
		this.clusterListeners.add(listener);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseCluster#addSynchronizationListener(net.sf.hajdbc.SynchronizationListener)
	 */
	@ManagedOperation
	@Override
	public void addSynchronizationListener(SynchronizationListener listener)
	{
		this.synchronizationListeners.add(listener);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseCluster#removeConfigurationListener(net.sf.hajdbc.DatabaseClusterConfigurationListener)
	 */
	@ManagedOperation
	@Override
	public void removeConfigurationListener(DatabaseClusterConfigurationListener<Z, D> listener)
	{
		this.configurationListeners.remove(listener);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseCluster#removeListener(net.sf.hajdbc.DatabaseClusterListener)
	 */
	@ManagedOperation
	@Override
	public void removeListener(DatabaseClusterListener listener)
	{
		this.clusterListeners.remove(listener);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseCluster#removeSynchronizationListener(net.sf.hajdbc.SynchronizationListener)
	 */
	@ManagedOperation
	@Override
	public void removeSynchronizationListener(SynchronizationListener listener)
	{
		this.synchronizationListeners.remove(listener);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseCluster#activate(net.sf.hajdbc.Database, net.sf.hajdbc.state.StateManager)
	 */
	@Override
	public boolean activate(D database, StateManager manager)
	{
		boolean added = this.balancer.add(database);
		
		if (added)
		{
			database.setActive(true);
			
			if (database.isDirty())
			{
				database.clean();
			}
			
			DatabaseEvent event = new DatabaseEvent(database);

			manager.activated(event);
			
			for (DatabaseClusterListener listener: this.clusterListeners)
			{
				listener.activated(event);
			}
		}
		
		return added;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseCluster#deactivate(net.sf.hajdbc.Database, net.sf.hajdbc.state.StateManager)
	 */
	@Override
	public boolean deactivate(D database, StateManager manager)
	{
		boolean removed = this.balancer.remove(database);
		
		if (removed)
		{
			database.setActive(false);
			
			DatabaseEvent event = new DatabaseEvent(database);

			manager.deactivated(event);
			
			for (DatabaseClusterListener listener: this.clusterListeners)
			{
				listener.deactivated(event);
			}
		}
		
		return removed;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseCluster#getBalancer()
	 */
	@Override
	public Balancer<Z, D> getBalancer()
	{
		return this.balancer;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseCluster#getDatabase(java.lang.String)
	 */
	@Override
	public D getDatabase(String id)
	{
		D database = this.configuration.getDatabaseMap().get(id);
		
		if (database == null)
		{
			throw new IllegalArgumentException(Messages.INVALID_DATABASE.getMessage(this, id));
		}
		
		return database;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseCluster#getDatabaseMetaDataCache()
	 */
	@Override
	public DatabaseMetaDataCache<Z, D> getDatabaseMetaDataCache()
	{
		return this.databaseMetaDataCache;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseCluster#getDialect()
	 */
	@Override
	public Dialect getDialect()
	{
		return this.dialect;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseCluster#getDurability()
	 */
	@Override
	public Durability<Z, D> getDurability()
	{
		return this.durability;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseCluster#getLockManager()
	 */
	@Override
	public LockManager getLockManager()
	{
		return this.lockManager;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseCluster#getExecutor()
	 */
	@Override
	public ExecutorService getExecutor()
	{
		return this.executor;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseCluster#getTransactionMode()
	 */
	@Override
	public TransactionMode getTransactionMode()
	{
		return this.configuration.getTransactionMode();
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseCluster#getStateManager()
	 */
	@Override
	public StateManager getStateManager()
	{
		return this.stateManager;
	}

	@Override
	public ThreadFactory getThreadFactory()
	{
		return this.configuration.getThreadFactory();
	}

	@Override
	public Decoder getDecoder()
	{
		return this.decoder;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseCluster#getTransactionIdentifierFactory()
	 */
	@Override
	public TransactionIdentifierFactory<? extends Object> getTransactionIdentifierFactory()
	{
		return this.configuration.getTransactionIdentifierFactory();
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseCluster#isCurrentDateEvaluationEnabled()
	 */
	@Override
	public boolean isCurrentDateEvaluationEnabled()
	{
		return this.configuration.isCurrentDateEvaluationEnabled();
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseCluster#isCurrentTimeEvaluationEnabled()
	 */
	@Override
	public boolean isCurrentTimeEvaluationEnabled()
	{
		return this.configuration.isCurrentTimeEvaluationEnabled();
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseCluster#isCurrentTimestampEvaluationEnabled()
	 */
	@Override
	public boolean isCurrentTimestampEvaluationEnabled()
	{
		return this.configuration.isCurrentTimestampEvaluationEnabled();
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseCluster#isIdentityColumnDetectionEnabled()
	 */
	@Override
	public boolean isIdentityColumnDetectionEnabled()
	{
		return this.configuration.isIdentityColumnDetectionEnabled();
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseCluster#isRandEvaluationEnabled()
	 */
	@Override
	public boolean isRandEvaluationEnabled()
	{
		return this.configuration.isRandEvaluationEnabled();
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseCluster#isSequenceDetectionEnabled()
	 */
	@Override
	public boolean isSequenceDetectionEnabled()
	{
		return this.configuration.isSequenceDetectionEnabled();
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.Lifecycle#start()
	 */
	@Override
	public synchronized void start() throws Exception
	{
		if (this.active) return;
		
		this.decoder = this.configuration.getDecoderFactory().createDecoder(this.id);
		this.lockManager = this.configuration.getLockManagerFactory().createLockManager();
		this.stateManager = this.configuration.getStateManagerFactory().createStateManager(this);
		
		CommandDispatcherFactory dispatcherFactory = this.configuration.getDispatcherFactory();
		
		if (dispatcherFactory != null)
		{
			this.lockManager = new DistributedLockManager(this, dispatcherFactory);
			this.stateManager = new DistributedStateManager<Z, D>(this, dispatcherFactory);
		}
		
		this.balancer = this.configuration.getBalancerFactory().createBalancer(new TreeSet<D>());
		this.dialect = this.configuration.getDialectFactory().createDialect();
		this.durability = this.configuration.getDurabilityFactory().createDurability(this);
		this.executor = this.configuration.getExecutorProvider().getExecutor(this.configuration.getThreadFactory());
		
		this.lockManager.start();
		this.stateManager.start();
		
		Set<String> databases = this.stateManager.getActiveDatabases();
		
		if (!databases.isEmpty())
		{
			for (String databaseId: databases)
			{
				D database = this.getDatabase(databaseId);
				
				if (database != null)
				{
					this.balancer.add(database);
				}
				else
				{
					logger.log(Level.WARN, Messages.DATABASE_IGNORED.getMessage(), this, databaseId);
				}
			}
		}
		else
		{
			for (D database: this.configuration.getDatabaseMap().values())
			{
				if (this.isAlive(database))
				{
					this.activate(database, this.stateManager);
				}
			}
		}

		this.durability.recover(this.stateManager.recover());
		
		this.databaseMetaDataCache = this.configuration.getDatabaseMetaDataCacheFactory().createCache(this);
		
		try
		{
			this.flushMetaDataCache();
		}
		catch (IllegalStateException e)
		{
			// Ignore - cache will initialize lazily.
		}
		
		CronExpression failureDetectionExpression = this.configuration.getFailureDetectionExpression();
		CronExpression autoActivationExpression = this.configuration.getAutoActivationExpression();
		int threads = requiredThreads(failureDetectionExpression) + requiredThreads(autoActivationExpression);
		
		if (threads > 0)
		{
			this.cronExecutor = new CronThreadPoolExecutor(threads, this.configuration.getThreadFactory());
			
			if (failureDetectionExpression != null)
			{
				this.cronExecutor.schedule(new FailureDetectionTask(), failureDetectionExpression);
			}
						
			if (autoActivationExpression != null)
			{
				this.cronExecutor.schedule(new AutoActivationTask(), autoActivationExpression);
			}
		}
		
		MBeanRegistrar<Z, D> registrar = this.configuration.getMBeanRegistrar();

		if (registrar != null)
		{
			registrar.register(this);
			
			for (D database: this.configuration.getDatabaseMap().values())
			{
				registrar.register(this, database);
			}
		}
		
		this.active = true;
	}

	private static int requiredThreads(CronExpression expression)
	{
		return (expression != null) ? 1 : 0;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.Lifecycle#stop()
	 */
	@Override
	public synchronized void stop()
	{
		this.active = false;
		
		MBeanRegistrar<Z, D> registrar = this.configuration.getMBeanRegistrar();

		if (registrar != null)
		{
			registrar.unregister(this);
			
			for (D database: this.configuration.getDatabaseMap().values())
			{
				registrar.unregister(this, database);
			}
		}
		
		if (this.cronExecutor != null)
		{
			this.cronExecutor.shutdownNow();
		}
		
		if (this.stateManager != null)
		{
			this.stateManager.stop();
		}
		
		if (this.lockManager != null)
		{
			this.lockManager.stop();
		}

		if (this.executor != null)
		{
			this.executor.shutdownNow();
		}

		if (this.balancer != null)
		{
			this.balancer.clear();
		}
	}

	boolean isAlive(D database)
	{
		try
		{
			Connection connection = database.connect(database.createConnectionSource(), database.decodePassword(this.decoder));
			try
			{
				return connection.isValid(0);
			}
			finally
			{
				Resources.close(connection);
			}
		}
		catch (SQLException e)
		{
			logger.log(Level.DEBUG, e);
			return false;
		}
	}

	boolean activate(D database, SynchronizationStrategy strategy) throws SQLException, InterruptedException
	{
		if (!this.isAlive(database)) return false;
		
		Lock lock = this.lockManager.writeLock(null);
		
		lock.lockInterruptibly();
		
		try
		{
			if (this.balancer.contains(database)) return false;
			
			if (!this.balancer.isEmpty())
			{
				SynchronizationContext<Z, D> context = new SynchronizationContextImpl<Z, D>(this, database);
				
				try
				{
					DatabaseEvent event = new DatabaseEvent(database);
					
					logger.log(Level.INFO, Messages.DATABASE_SYNC_START.getMessage(this, database));
					
					for (SynchronizationListener listener: this.synchronizationListeners)
					{
						listener.beforeSynchronization(event);
					}
					
					strategy.synchronize(context);
	
					logger.log(Level.INFO, Messages.DATABASE_SYNC_END.getMessage(this, database));
					
					for (SynchronizationListener listener: this.synchronizationListeners)
					{
						listener.afterSynchronization(event);
					}
				}
				finally
				{
					context.close();
				}
			}
			
			return this.activate(database, this.stateManager);
		}
		finally
		{
			lock.unlock();
		}
	}

	class FailureDetectionTask implements Runnable
	{
		@Override
		public void run()
		{
			if (!DatabaseClusterImpl.this.getStateManager().isEnabled()) return;
			
			Set<D> databases = DatabaseClusterImpl.this.getBalancer();
			
			int size = databases.size();
			
			if ((size > 1) || DatabaseClusterImpl.this.configuration.isEmptyClusterAllowed())
			{
				List<D> deadList = new ArrayList<D>(size);
				
				for (D database: databases)
				{
					if (!DatabaseClusterImpl.this.isAlive(database))
					{
						deadList.add(database);
					}
				}

				if ((deadList.size() < size) || DatabaseClusterImpl.this.configuration.isEmptyClusterAllowed())
				{
					for (D database: deadList)
					{
						if (DatabaseClusterImpl.this.deactivate(database, DatabaseClusterImpl.this.getStateManager()))
						{
							logger.log(Level.ERROR, Messages.DATABASE_DEACTIVATED.getMessage(), database, DatabaseClusterImpl.this);
						}
					}
				}
			}
		}
	}	
	
	class AutoActivationTask implements Runnable
	{
		@Override
		public void run()
		{
			if (!DatabaseClusterImpl.this.getStateManager().isEnabled()) return;
			
			try
			{
				Set<D> activeDatabases = DatabaseClusterImpl.this.getBalancer();
				
				if (!activeDatabases.isEmpty())
				{
					for (D database: DatabaseClusterImpl.this.configuration.getDatabaseMap().values())
					{
						if (!activeDatabases.contains(database))
						{
							try
							{
								if (DatabaseClusterImpl.this.activate(database, DatabaseClusterImpl.this.configuration.getSynchronizationStrategyMap().get(DatabaseClusterImpl.this.configuration.getDefaultSynchronizationStrategy())))
								{
									logger.log(Level.INFO, Messages.DATABASE_ACTIVATED.getMessage(), database, DatabaseClusterImpl.this);
								}
							}
							catch (SQLException e)
							{
								logger.log(Level.DEBUG, e);
							}
						}
					}
				}
			}
			catch (InterruptedException e)
			{
				Thread.currentThread().interrupt();
			}
		}
	}
}
