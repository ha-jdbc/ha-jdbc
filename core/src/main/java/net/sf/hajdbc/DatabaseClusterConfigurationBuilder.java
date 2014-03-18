package net.sf.hajdbc;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import net.sf.hajdbc.balancer.BalancerFactory;
import net.sf.hajdbc.balancer.load.LoadBalancerFactory;
import net.sf.hajdbc.cache.DatabaseMetaDataCacheFactory;
import net.sf.hajdbc.cache.eager.EagerDatabaseMetaDataCacheFactory;
import net.sf.hajdbc.codec.DecoderFactory;
import net.sf.hajdbc.codec.MultiplexingDecoderFactory;
import net.sf.hajdbc.configuration.Builder;
import net.sf.hajdbc.configuration.ServiceBuilder;
import net.sf.hajdbc.configuration.SimpleBuilder;
import net.sf.hajdbc.configuration.SimpleServiceBuilder;
import net.sf.hajdbc.dialect.DialectFactory;
import net.sf.hajdbc.dialect.StandardDialectFactory;
import net.sf.hajdbc.distributed.CommandDispatcherFactory;
import net.sf.hajdbc.durability.DurabilityFactory;
import net.sf.hajdbc.durability.coarse.CoarseDurabilityFactory;
import net.sf.hajdbc.io.InputSinkProvider;
import net.sf.hajdbc.io.file.FileInputSinkProvider;
import net.sf.hajdbc.lock.LockManagerFactory;
import net.sf.hajdbc.lock.semaphore.SemaphoreLockManagerFactory;
import net.sf.hajdbc.management.DefaultMBeanRegistrarFactory;
import net.sf.hajdbc.management.MBeanRegistrarFactory;
import net.sf.hajdbc.sql.DefaultExecutorServiceProvider;
import net.sf.hajdbc.sql.TransactionModeEnum;
import net.sf.hajdbc.state.StateManagerFactory;
import net.sf.hajdbc.state.sql.SQLStateManagerFactory;
import net.sf.hajdbc.util.concurrent.cron.CronExpression;
import net.sf.hajdbc.util.concurrent.cron.CronExpressionBuilder;

public class DatabaseClusterConfigurationBuilder<Z, D extends Database<Z>, B extends DatabaseBuilder<Z, D>> implements Builder<DatabaseClusterConfiguration<Z, D>>
{
	private final DatabaseBuilderFactory<Z, D, B> factory;
	
	private final List<Builder<SynchronizationStrategy>> synchronizationStrategyBuilders = new LinkedList<>();
	private final List<Builder<D>> databaseConfigurationBuilders = new LinkedList<>();

	private volatile Builder<CommandDispatcherFactory> commandDispatcherFactoryBuilder;
	private volatile Builder<StateManagerFactory> stateManagerFactoryBuilder = new SimpleBuilder<StateManagerFactory>(new SQLStateManagerFactory());
	private volatile Builder<LockManagerFactory> lockManagerFactoryBuilder = new SimpleBuilder<LockManagerFactory>(new SemaphoreLockManagerFactory());
	private volatile Builder<BalancerFactory> balancerFactoryBuilder = new SimpleBuilder<BalancerFactory>(new LoadBalancerFactory());
	private volatile Builder<DialectFactory> dialectFactoryBuilder = new SimpleBuilder<DialectFactory>(new StandardDialectFactory());
	private volatile Builder<DurabilityFactory> durabilityFactoryBuilder = new SimpleBuilder<DurabilityFactory>(new CoarseDurabilityFactory());
	private volatile Builder<InputSinkProvider> inputSinkProviderBuilder = new SimpleBuilder<InputSinkProvider>(new FileInputSinkProvider());
	private volatile Builder<DatabaseMetaDataCacheFactory> metaDataCacheFactoryBuilder = new SimpleBuilder<DatabaseMetaDataCacheFactory>(new EagerDatabaseMetaDataCacheFactory());

	private volatile Builder<DecoderFactory> decoderFactoryBuilder = new SimpleBuilder<DecoderFactory>(new MultiplexingDecoderFactory());
	private volatile Builder<MBeanRegistrarFactory> mbeanRegistrarFactoryBuilder = new SimpleBuilder<MBeanRegistrarFactory>(new DefaultMBeanRegistrarFactory());
	private volatile Builder<ThreadFactory> threadFactoryBuilder = new SimpleBuilder<>(Executors.defaultThreadFactory());
	private volatile Builder<ExecutorServiceProvider> executorProviderBuilder = new SimpleBuilder<ExecutorServiceProvider>(new DefaultExecutorServiceProvider());

	private volatile CronExpressionBuilder autoActivateScheduleBuilder = new CronExpressionBuilder();
	private volatile CronExpressionBuilder failureDetectScheduleBuilder = new CronExpressionBuilder();
	
	private volatile String defaultSynchronizationStrategy;
	private volatile TransactionModeEnum transactionMode = TransactionModeEnum.SERIAL;
	private volatile boolean evalCurrentDate = false;
	private volatile boolean evalCurrentTime = false;
	private volatile boolean evalCurrentTimestamp = false;
	private volatile boolean evalRand = false;
	private volatile boolean detectIdentityColumns = false;
	private volatile boolean detectSequences = false;
	private volatile boolean allowEmptyCluster = false;
	
	protected DatabaseClusterConfigurationBuilder(DatabaseBuilderFactory<Z, D, B> factory)
	{
		this.factory = factory;
	}

	public ServiceBuilder<CommandDispatcherFactory> distributable(String id)
	{
		ServiceBuilder<CommandDispatcherFactory> builder = new ServiceBuilder<>(CommandDispatcherFactory.class, id);
		this.commandDispatcherFactoryBuilder = builder;
		return builder;
	}
	
	public <T extends Builder<CommandDispatcherFactory>> T distributable(Class<T> builderClass)
	{
		try
		{
			T builder = builderClass.newInstance();
			this.commandDispatcherFactoryBuilder = builder;
			return builder;
		}
		catch (IllegalAccessException | InstantiationException e)
		{
			throw new IllegalStateException(e);
		}
	}

	public DatabaseClusterConfigurationBuilder<Z, D, B> distributable(CommandDispatcherFactory factory)
	{
		this.commandDispatcherFactoryBuilder = new SimpleBuilder<>(factory);
		return this;
	}

	public ServiceBuilder<StateManagerFactory> state(String id)
	{
		ServiceBuilder<StateManagerFactory> builder = new ServiceBuilder<>(StateManagerFactory.class, id);
		this.stateManagerFactoryBuilder = builder;
		return builder;
	}

	public <T extends Builder<StateManagerFactory>> T state(Class<T> builderClass)
	{
		try
		{
			T builder = builderClass.newInstance();
			this.stateManagerFactoryBuilder = builder;
			return builder;
		}
		catch (IllegalAccessException | InstantiationException e)
		{
			throw new IllegalStateException(e);
		}
	}

	public DatabaseClusterConfigurationBuilder<Z, D, B> state(StateManagerFactory factory)
	{
		this.stateManagerFactoryBuilder = new SimpleBuilder<>(factory);
		return this;
	}

	public ServiceBuilder<LockManagerFactory> lock(String id)
	{
		ServiceBuilder<LockManagerFactory> builder = new ServiceBuilder<>(LockManagerFactory.class, id);
		this.lockManagerFactoryBuilder = builder;
		return builder;
	}

	public <T extends Builder<LockManagerFactory>> T lock(Class<T> builderClass)
	{
		try
		{
			T builder = builderClass.newInstance();
			this.lockManagerFactoryBuilder = builder;
			return builder;
		}
		catch (IllegalAccessException | InstantiationException e)
		{
			throw new IllegalStateException(e);
		}
	}

	public DatabaseClusterConfigurationBuilder<Z, D, B> lock(LockManagerFactory factory)
	{
		this.lockManagerFactoryBuilder = new SimpleBuilder<>(factory);
		return this;
	}

	public ServiceBuilder<SynchronizationStrategy> addSynchronizationStrategy(String id)
	{
		ServiceBuilder<SynchronizationStrategy> builder = new ServiceBuilder<>(SynchronizationStrategy.class, id);
		this.synchronizationStrategyBuilders.add(builder);
		return builder;
	}

	public <T extends Builder<SynchronizationStrategy>> T addSynchronizationStrategy(Class<T> builderClass)
	{
		try
		{
			T builder = builderClass.newInstance();
			this.synchronizationStrategyBuilders.add(builder);
			return builder;
		}
		catch (IllegalAccessException | InstantiationException e)
		{
			throw new IllegalStateException(e);
		}
	}

	public DatabaseClusterConfigurationBuilder<Z, D, B> addSynchronizationStrategy(SynchronizationStrategy strategy)
	{
		this.synchronizationStrategyBuilders.add(new SimpleBuilder<>(strategy));
		return this;
	}

	public B addDatabase(String id)
	{
		B builder = this.factory.createBuilder(id);
		this.databaseConfigurationBuilders.add(builder);
		return builder;
	}
	
	public <T extends Builder<ExecutorServiceProvider>> T executor(Class<T> builderClass)
	{
		try
		{
			T builder = builderClass.newInstance();
			this.executorProviderBuilder = builder;
			return builder;
		}
		catch (IllegalAccessException | InstantiationException e)
		{
			throw new IllegalStateException(e);
		}
	}

	public DatabaseClusterConfigurationBuilder<Z, D, B> executor(ExecutorServiceProvider provider)
	{
		this.executorProviderBuilder = new SimpleBuilder<>(provider);
		return this;
	}

	public DatabaseClusterConfigurationBuilder<Z, D, B> balancer(String id)
	{
		this.balancerFactoryBuilder = new SimpleServiceBuilder<>(BalancerFactory.class, id);
		return this;
	}

	public DatabaseClusterConfigurationBuilder<Z, D, B> balancer(BalancerFactory factory)
	{
		this.balancerFactoryBuilder = new SimpleBuilder<>(factory);
		return this;
	}

	public DatabaseClusterConfigurationBuilder<Z, D, B> dialect(String id)
	{
		this.dialectFactoryBuilder = new SimpleServiceBuilder<>(DialectFactory.class, id);
		return this;
	}

	public DatabaseClusterConfigurationBuilder<Z, D, B> dialect(DialectFactory factory)
	{
		this.dialectFactoryBuilder = new SimpleBuilder<>(factory);
		return this;
	}
	
	public DatabaseClusterConfigurationBuilder<Z, D, B> durability(String id)
	{
		this.durabilityFactoryBuilder = new SimpleServiceBuilder<>(DurabilityFactory.class, id);
		return this;
	}

	public DatabaseClusterConfigurationBuilder<Z, D, B> durability(DurabilityFactory factory)
	{
		this.durabilityFactoryBuilder = new SimpleBuilder<>(factory);
		return this;
	}

	public DatabaseClusterConfigurationBuilder<Z, D, B> durability(InputSinkProvider provider)
	{
		this.inputSinkProviderBuilder = new SimpleBuilder<>(provider);
		return this;
	}
	
	public DatabaseClusterConfigurationBuilder<Z, D, B> metaDataCache(String id)
	{
		this.metaDataCacheFactoryBuilder = new SimpleServiceBuilder<>(DatabaseMetaDataCacheFactory.class, id);
		return this;
	}

	public DatabaseClusterConfigurationBuilder<Z, D, B> metaDataCache(DatabaseMetaDataCacheFactory factory)
	{
		this.metaDataCacheFactoryBuilder = new SimpleBuilder<>(factory);
		return this;
	}
	
	public DatabaseClusterConfigurationBuilder<Z, D, B> inputSink(String id)
	{
		this.inputSinkProviderBuilder = new SimpleServiceBuilder<>(InputSinkProvider.class, id);
		return this;
	}
	
	public DatabaseClusterConfigurationBuilder<Z, D, B> inputSink(InputSinkProvider factory)
	{
		this.inputSinkProviderBuilder = new SimpleBuilder<>(factory);
		return this;
	}
	
	public DatabaseClusterConfigurationBuilder<Z, D, B> decoder(DecoderFactory factory)
	{
		this.decoderFactoryBuilder = new SimpleBuilder<>(factory);
		return this;
	}
	
	public DatabaseClusterConfigurationBuilder<Z, D, B> mbeanRegistrar(MBeanRegistrarFactory factory)
	{
		this.mbeanRegistrarFactoryBuilder = new SimpleBuilder<>(factory);
		return this;
	}
	
	public DatabaseClusterConfigurationBuilder<Z, D, B> defaultSynchronizationStrategy(String id)
	{
		this.defaultSynchronizationStrategy = id;
		return this;
	}
	
	public DatabaseClusterConfigurationBuilder<Z, D, B> transactionMode(TransactionModeEnum transactionMode)
	{
		this.transactionMode = transactionMode;
		return this;
	}

	public DatabaseClusterConfigurationBuilder<Z, D, B> autoActivateSchedule(String schedule)
	{
		this.autoActivateScheduleBuilder.expression(schedule);
		return this;
	}

	public DatabaseClusterConfigurationBuilder<Z, D, B> failureDetectSchedule(String schedule)
	{
		this.failureDetectScheduleBuilder.expression(schedule);
		return this;
	}

	public DatabaseClusterConfigurationBuilder<Z, D, B> evalCurrentDate(boolean enabled)
	{
		this.evalCurrentDate = enabled;
		return this;
	}

	public DatabaseClusterConfigurationBuilder<Z, D, B> evalCurrentTime(boolean enabled)
	{
		this.evalCurrentTime = enabled;
		return this;
	}

	public DatabaseClusterConfigurationBuilder<Z, D, B> evalCurrentTimestamp(boolean enabled)
	{
		this.evalCurrentTimestamp = enabled;
		return this;
	}

	public DatabaseClusterConfigurationBuilder<Z, D, B> evalRand(boolean enabled)
	{
		this.evalRand = enabled;
		return this;
	}

	public DatabaseClusterConfigurationBuilder<Z, D, B> detectIdentityColumns(boolean enabled)
	{
		this.detectIdentityColumns = enabled;
		return this;
	}

	public DatabaseClusterConfigurationBuilder<Z, D, B> detectSequences(boolean enabled)
	{
		this.detectSequences = enabled;
		return this;
	}

	public DatabaseClusterConfigurationBuilder<Z, D, B> allowEmptyCluster(boolean enabled)
	{
		this.allowEmptyCluster = enabled;
		return this;
	}

	@Override
	public DatabaseClusterConfigurationBuilder<Z, D, B> read(DatabaseClusterConfiguration<Z, D> configuration)
	{
		return this;
	}

	@Override
	public DatabaseClusterConfiguration<Z, D> build() throws SQLException
	{
		final CommandDispatcherFactory commandDispatcherFactory = (this.commandDispatcherFactoryBuilder != null) ? this.commandDispatcherFactoryBuilder.build() : null;
		final StateManagerFactory stateManagerFactory = this.stateManagerFactoryBuilder.build();
		final LockManagerFactory lockManagerFactory = this.lockManagerFactoryBuilder.build();
		final BalancerFactory balancerFactory = this.balancerFactoryBuilder.build();
		final DialectFactory dialectFactory = this.dialectFactoryBuilder.build();
		final DurabilityFactory durabilityFactory = this.durabilityFactoryBuilder.build();
		final InputSinkProvider inputSinkProvider = this.inputSinkProviderBuilder.build();
		final DatabaseMetaDataCacheFactory metaDataCacheFactory = this.metaDataCacheFactoryBuilder.build();
		final DecoderFactory decoderFactory = this.decoderFactoryBuilder.build();
		final MBeanRegistrarFactory mbeanRegistrarFactory = this.mbeanRegistrarFactoryBuilder.build();
		final ThreadFactory threadFactory = this.threadFactoryBuilder.build();
		final ExecutorServiceProvider executorServiceProvider = this.executorProviderBuilder.build();
		final CronExpression autoActivateSchedule = this.autoActivateScheduleBuilder.build();
		final CronExpression failureDetectSchedule = this.failureDetectScheduleBuilder.build();
		
		final String defaultSynchronizationStrategy = this.defaultSynchronizationStrategy;
		final TransactionModeEnum transactionMode = this.transactionMode;
		final boolean evalCurrentDate = this.evalCurrentDate;
		final boolean evalCurrentTime = this.evalCurrentTime;
		final boolean evalCurrentTimestamp = this.evalCurrentTimestamp;
		final boolean evalRand = this.evalRand;
		final boolean detectIdentityColumns = this.detectIdentityColumns;
		final boolean detectSequences = this.detectSequences;
		final boolean allowEmptyCluster = this.allowEmptyCluster;

		final Map<String, SynchronizationStrategy> syncStrategies = new HashMap<>();
		for (Builder<SynchronizationStrategy> builder: this.synchronizationStrategyBuilders)
		{
			SynchronizationStrategy strategy = builder.build();
			syncStrategies.put(strategy.getId(), strategy);
		}
		
		if (!syncStrategies.containsKey(defaultSynchronizationStrategy))
		{
			throw new SQLException(Messages.INVALID_SYNC_STRATEGY.getMessage(defaultSynchronizationStrategy));
		}
		
		final ConcurrentMap<String, D> databases = new ConcurrentHashMap<>();
		for (Builder<D> builder: this.databaseConfigurationBuilders)
		{
			D database = builder.build();
			databases.put(database.getId(), database);
		}
		
		return new DatabaseClusterConfiguration<Z, D>()
		{
			@Override
			public CommandDispatcherFactory getDispatcherFactory()
			{
				return commandDispatcherFactory;
			}

			@Override
			public ConcurrentMap<String, D> getDatabaseMap()
			{
				return databases;
			}

			@Override
			public Map<String, SynchronizationStrategy> getSynchronizationStrategyMap()
			{
				return syncStrategies;
			}

			@Override
			public String getDefaultSynchronizationStrategy()
			{
				return defaultSynchronizationStrategy;
			}

			@Override
			public BalancerFactory getBalancerFactory()
			{
				return balancerFactory;
			}

			@Override
			public TransactionMode getTransactionMode()
			{
				return transactionMode;
			}

			@Override
			public ExecutorServiceProvider getExecutorProvider()
			{
				return executorServiceProvider;
			}

			@Override
			public DialectFactory getDialectFactory()
			{
				return dialectFactory;
			}

			@Override
			public StateManagerFactory getStateManagerFactory()
			{
				return stateManagerFactory;
			}

			@Override
			public DatabaseMetaDataCacheFactory getDatabaseMetaDataCacheFactory()
			{
				return metaDataCacheFactory;
			}

			@Override
			public DurabilityFactory getDurabilityFactory()
			{
				return durabilityFactory;
			}

			@Override
			public LockManagerFactory getLockManagerFactory()
			{
				return lockManagerFactory;
			}

			@Override
			public boolean isSequenceDetectionEnabled()
			{
				return detectSequences;
			}

			@Override
			public boolean isIdentityColumnDetectionEnabled()
			{
				return detectIdentityColumns;
			}

			@Override
			public boolean isCurrentDateEvaluationEnabled()
			{
				return evalCurrentDate;
			}

			@Override
			public boolean isCurrentTimeEvaluationEnabled()
			{
				return evalCurrentTime;
			}

			@Override
			public boolean isCurrentTimestampEvaluationEnabled()
			{
				return evalCurrentTimestamp;
			}

			@Override
			public boolean isRandEvaluationEnabled()
			{
				return evalRand;
			}

			@Override
			public CronExpression getFailureDetectionExpression()
			{
				return failureDetectSchedule;
			}

			@Override
			public CronExpression getAutoActivationExpression()
			{
				return autoActivateSchedule;
			}

			@Override
			public ThreadFactory getThreadFactory()
			{
				return threadFactory;
			}

			@Override
			public DecoderFactory getDecoderFactory()
			{
				return decoderFactory;
			}

			@Override
			public MBeanRegistrarFactory getMBeanRegistrarFactory()
			{
				return mbeanRegistrarFactory;
			}

			@Override
			public boolean isEmptyClusterAllowed()
			{
				return allowEmptyCluster;
			}

			@Override
			public InputSinkProvider getInputSinkProvider()
			{
				return inputSinkProvider;
			}
		};
	}
}
