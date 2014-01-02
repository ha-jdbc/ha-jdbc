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

import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.beans.PropertyEditor;
import java.beans.PropertyEditorManager;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlID;
import javax.xml.bind.annotation.XmlIDREF;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.XmlValue;
import javax.xml.bind.annotation.adapters.XmlAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseClusterConfiguration;
import net.sf.hajdbc.DatabaseFactory;
import net.sf.hajdbc.ExecutorServiceProvider;
import net.sf.hajdbc.Identifiable;
import net.sf.hajdbc.IdentifiableMatcher;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.TransactionMode;
import net.sf.hajdbc.balancer.BalancerFactory;
import net.sf.hajdbc.cache.DatabaseMetaDataCacheFactory;
import net.sf.hajdbc.codec.DecoderFactory;
import net.sf.hajdbc.codec.MultiplexingDecoderFactory;
import net.sf.hajdbc.dialect.DialectFactory;
import net.sf.hajdbc.distributed.CommandDispatcherFactory;
import net.sf.hajdbc.durability.DurabilityFactory;
import net.sf.hajdbc.io.InputSinkProvider;
import net.sf.hajdbc.lock.LockManagerFactory;
import net.sf.hajdbc.management.DefaultMBeanRegistrar;
import net.sf.hajdbc.management.MBeanRegistrar;
import net.sf.hajdbc.state.StateManagerFactory;
import net.sf.hajdbc.tx.SimpleTransactionIdentifierFactory;
import net.sf.hajdbc.tx.TransactionIdentifierFactory;
import net.sf.hajdbc.tx.UUIDTransactionIdentifierFactory;
import net.sf.hajdbc.util.ServiceLoaders;
import net.sf.hajdbc.util.concurrent.cron.CronExpression;

/**
 * @author Paul Ferraro
 */
@XmlType(propOrder = { "commandDispatcherFactoryDescriptor", "synchronizationStrategyDescriptors", "stateManagerFactoryDescriptor", "lockManagerFactoryDescriptor" })
public abstract class AbstractDatabaseClusterConfiguration<Z, D extends Database<Z>> implements DatabaseClusterConfiguration<Z, D>
{
	private static final long serialVersionUID = -2808296483725374829L;

	private CommandDispatcherFactory dispatcherFactory;	
	private Map<String, SynchronizationStrategy> synchronizationStrategies = new HashMap<>();
	private StateManagerFactory stateManagerFactory = ServiceLoaders.findRequiredService(StateManagerFactory.class);
	private LockManagerFactory lockManagerFactory = ServiceLoaders.findRequiredService(LockManagerFactory.class);

	protected abstract NestedConfiguration<Z, D> getNestedConfiguration();

	@Override
	public DatabaseFactory<Z, D> getDatabaseFactory()
	{
		return this.getNestedConfiguration().getDatabaseFactory();
	}

	@XmlElement(name = "distributable")
	private CommandDispatcherFactoryDescriptor getCommandDispatcherFactoryDescriptor() throws Exception
	{
		return (this.dispatcherFactory != null) ? new CommandDispatcherFactoryDescriptorAdapter().marshal(this.dispatcherFactory) : null;
	}

	@SuppressWarnings("unused")
	private void setCommandDispatcherFactoryDescriptor(CommandDispatcherFactoryDescriptor descriptor) throws Exception
	{
		this.dispatcherFactory = (descriptor != null) ? new CommandDispatcherFactoryDescriptorAdapter().unmarshal(descriptor) : null;
	}
	
	@XmlElement(name = "sync")
	private SynchronizationStrategyDescriptor[] getSynchronizationStrategyDescriptors() throws Exception
	{
		List<SynchronizationStrategyDescriptor> results = new ArrayList<>(this.synchronizationStrategies.size());
		SynchronizationStrategyDescriptorAdapter adapter = new SynchronizationStrategyDescriptorAdapter();

		for (Map.Entry<String, SynchronizationStrategy> entry: this.synchronizationStrategies.entrySet())
		{
			SynchronizationStrategyDescriptor result = adapter.marshal(entry.getValue());
			
			result.setId(entry.getKey());
			
			results.add(result);
		}
		
		return results.toArray(new SynchronizationStrategyDescriptor[results.size()]);
	}
	
	@SuppressWarnings("unused")
	private void setSynchronizationStrategyDescriptors(SynchronizationStrategyDescriptor[] entries) throws Exception
	{
		SynchronizationStrategyDescriptorAdapter adapter = new SynchronizationStrategyDescriptorAdapter();
		
		for (SynchronizationStrategyDescriptor entry: entries)
		{
			SynchronizationStrategy strategy = adapter.unmarshal(entry);
			
			this.synchronizationStrategies.put(entry.getId(), strategy);
		}
	}
	
	@XmlElement(name = "state")
	private StateManagerFactoryDescriptor getStateManagerFactoryDescriptor() throws Exception
	{
		return new StateManagerFactoryDescriptorAdapter().marshal(this.stateManagerFactory);
	}
	
	@SuppressWarnings("unused")
	private void setStateManagerFactoryDescriptor(StateManagerFactoryDescriptor descriptor) throws Exception
	{
		this.stateManagerFactory = new StateManagerFactoryDescriptorAdapter().unmarshal(descriptor);
	}
	
	@XmlElement(name = "lock")
	private LockManagerFactoryDescriptor getLockManagerFactoryDescriptor() throws Exception
	{
		return new LockManagerFactoryDescriptorAdapter().marshal(this.lockManagerFactory);
	}
	
	@SuppressWarnings("unused")
	private void setLockManagerFactoryDescriptor(LockManagerFactoryDescriptor descriptor) throws Exception
	{
		this.lockManagerFactory = new LockManagerFactoryDescriptorAdapter().unmarshal(descriptor);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterConfiguration#getAutoActivationExpression()
	 */
	@Override
	public CronExpression getAutoActivationExpression()
	{
		return this.getNestedConfiguration().getAutoActivationExpression();
	}

	public void setAutoActivationExpression(CronExpression expression)
	{
		this.getNestedConfiguration().setAutoActivationExpression(expression);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterConfiguration#getBalancerFactory()
	 */
	@Override
	public BalancerFactory getBalancerFactory()
	{
		return this.getNestedConfiguration().getBalancerFactory();
	}

	public void setBalancerFactory(BalancerFactory factory)
	{
		this.getNestedConfiguration().setBalancerFactory(factory);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterConfiguration#getDispatcherFactory()
	 */
	@Override
	public CommandDispatcherFactory getDispatcherFactory()
	{
		return this.dispatcherFactory;
	}

	public void setDispatcherFactory(CommandDispatcherFactory factory)
	{
		this.dispatcherFactory = factory;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterConfiguration#getDatabaseMap()
	 */
	@Override
	public ConcurrentMap<String, D> getDatabaseMap()
	{
		return this.getNestedConfiguration().getDatabaseMap();
	}

	public void setDatabases(Collection<D> databases)
	{
		Map<String, D> map = this.getDatabaseMap();
		
		for (D database: databases)
		{
			map.put(database.getId(), database);
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterConfiguration#getDatabaseMetaDataCacheFactory()
	 */
	@Override
	public DatabaseMetaDataCacheFactory getDatabaseMetaDataCacheFactory()
	{
		return this.getNestedConfiguration().getDatabaseMetaDataCacheFactory();
	}

	public void setDatabaseMetaDataCacheFactory(DatabaseMetaDataCacheFactory factory)
	{
		this.getNestedConfiguration().setDatabaseMetaDataCacheFactory(factory);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterConfiguration#getDefaultSynchronizationStrategy()
	 */
	@Override
	public String getDefaultSynchronizationStrategy()
	{
		return this.getNestedConfiguration().getDefaultSynchronizationStrategy();
	}

	public void setDefaultSynchronizationStrategy(String strategy)
	{
		this.getNestedConfiguration().setDefaultSynchronizationStrategy(strategy);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterConfiguration#getDialectFactory()
	 */
	@Override
	public DialectFactory getDialectFactory()
	{
		return this.getNestedConfiguration().getDialectFactory();
	}

	public void setDialectFactory(DialectFactory factory)
	{
		this.getNestedConfiguration().setDialectFactory(factory);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterConfiguration#getDurabilityFactory()
	 */
	@Override
	public DurabilityFactory getDurabilityFactory()
	{
		return this.getNestedConfiguration().getDurabilityFactory();
	}

	public void setDurabilityFactory(DurabilityFactory factory)
	{
		this.getNestedConfiguration().setDurabilityFactory(factory);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterConfiguration#getExecutorProvider()
	 */
	@Override
	public ExecutorServiceProvider getExecutorProvider()
	{
		return this.getNestedConfiguration().getExecutorProvider();
	}
	
	public void setExecutorProvider(ExecutorServiceProvider provider)
	{
		this.getNestedConfiguration().setExecutorProvider(provider);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterConfiguration#getThreadFactory()
	 */
	@Override
	public ThreadFactory getThreadFactory()
	{
		return this.getNestedConfiguration().getThreadFactory();
	}

	public void setThreadFactory(ThreadFactory factory)
	{
		this.getNestedConfiguration().setThreadFactory(factory);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterConfiguration#getDecoderFactory()
	 */
	@Override
	public DecoderFactory getDecoderFactory()
	{
		return this.getNestedConfiguration().getDecoderFactory();
	}

	public void setCodecFactory(DecoderFactory factory)
	{
		this.getNestedConfiguration().setDecoderFactory(factory);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterConfiguration#getFailureDetectionExpression()
	 */
	@Override
	public CronExpression getFailureDetectionExpression()
	{
		return this.getNestedConfiguration().getFailureDetectionExpression();
	}

	public void setFailureDetectionExpression(CronExpression expression)
	{
		this.getNestedConfiguration().setFailureDetectionExpression(expression);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterConfiguration#getStateManagerFactory()
	 */
	@Override
	public StateManagerFactory getStateManagerFactory()
	{
		return this.stateManagerFactory;
	}

	public void setStateManagerFactory(StateManagerFactory factory)
	{
		this.stateManagerFactory = factory;
	}
	
	@Override
	public LockManagerFactory getLockManagerFactory()
	{
		return this.lockManagerFactory;
	}

	public void setLockManagerFactory(LockManagerFactory factory)
	{
		this.lockManagerFactory = factory;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterConfiguration#getSynchronizationStrategyMap()
	 */
	@Override
	public Map<String, SynchronizationStrategy> getSynchronizationStrategyMap()
	{
		return this.synchronizationStrategies;
	}

	public void setSynchronizationStrategyMap(Map<String, SynchronizationStrategy> strategies)
	{
		this.synchronizationStrategies = strategies;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterConfiguration#getTransactionMode()
	 */
	@Override
	public TransactionMode getTransactionMode()
	{
		return this.getNestedConfiguration().getTransactionMode();
	}

	public void setTransactionMode(TransactionMode mode)
	{
		this.getNestedConfiguration().setTransactionMode(mode);
	}
	
	@Override
	public InputSinkProvider getInputSinkProvider()
	{
		return this.getNestedConfiguration().getInputSinkProvider();
	}

	public void setInputSinkFactoryProvider(InputSinkProvider provider)
	{
		this.getNestedConfiguration().setSinkSourceProvider(provider);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterConfiguration#isCurrentDateEvaluationEnabled()
	 */
	@Override
	public boolean isCurrentDateEvaluationEnabled()
	{
		return this.getNestedConfiguration().isCurrentDateEvaluationEnabled();
	}

	public void setCurrentDateEvaluationEnabled(boolean enabled)
	{
		this.getNestedConfiguration().setCurrentDateEvaluationEnabled(enabled);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterConfiguration#isCurrentTimeEvaluationEnabled()
	 */
	@Override
	public boolean isCurrentTimeEvaluationEnabled()
	{
		return this.getNestedConfiguration().isCurrentTimeEvaluationEnabled();
	}

	public void setCurrentTimeEvaluationEnabled(boolean enabled)
	{
		this.getNestedConfiguration().setCurrentTimeEvaluationEnabled(enabled);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterConfiguration#isCurrentTimestampEvaluationEnabled()
	 */
	@Override
	public boolean isCurrentTimestampEvaluationEnabled()
	{
		return this.getNestedConfiguration().isCurrentTimestampEvaluationEnabled();
	}

	public void setCurrentTimestampEvaluationEnabled(boolean enabled)
	{
		this.getNestedConfiguration().setCurrentTimestampEvaluationEnabled(enabled);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterConfiguration#isIdentityColumnDetectionEnabled()
	 */
	@Override
	public boolean isIdentityColumnDetectionEnabled()
	{
		return this.getNestedConfiguration().isIdentityColumnDetectionEnabled();
	}

	public void setIdentityColumnDetectionEnabled(boolean enabled)
	{
		this.getNestedConfiguration().setIdentityColumnDetectionEnabled(enabled);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterConfiguration#isRandEvaluationEnabled()
	 */
	@Override
	public boolean isRandEvaluationEnabled()
	{
		return this.getNestedConfiguration().isRandEvaluationEnabled();
	}

	public void setRandEvaluationEnabled(boolean enabled)
	{
		this.getNestedConfiguration().setRandEvaluationEnabled(enabled);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterConfiguration#isSequenceDetectionEnabled()
	 */
	@Override
	public boolean isSequenceDetectionEnabled()
	{
		return this.getNestedConfiguration().isSequenceDetectionEnabled();
	}
	
	public void setSequenceDetectionEnabled(boolean enabled)
	{
		this.getNestedConfiguration().setSequenceDetectionEnabled(enabled);
	}
	
	static Map<String, Map.Entry<PropertyDescriptor, PropertyEditor>> findDescriptors(Class<?> targetClass) throws Exception
	{
		Map<String, Map.Entry<PropertyDescriptor, PropertyEditor>> map = new HashMap<>();
		
		for (PropertyDescriptor descriptor: Introspector.getBeanInfo(targetClass).getPropertyDescriptors())
		{
			if ((descriptor.getReadMethod() != null) && (descriptor.getWriteMethod() != null))
			{
				PropertyEditor editor = PropertyEditorManager.findEditor(descriptor.getPropertyType());
				
				if (editor != null)
				{
					map.put(descriptor.getName(), new AbstractMap.SimpleImmutableEntry<>(descriptor, editor));
				}
			}
		}
		
		return map;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterConfiguration#getMBeanRegistrar()
	 */
	@Override
	public MBeanRegistrar<Z, D> getMBeanRegistrar()
	{
		return this.getNestedConfiguration().getMBeanRegistrar();
	}

	public void setMBeanRegistrar(MBeanRegistrar<Z, D> registrar)
	{
		this.getNestedConfiguration().setMBeanRegistrar(registrar);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterConfiguration#isEmptyClusterAllowed()
	 */
	@Override
	public boolean isEmptyClusterAllowed()
	{
		return this.getNestedConfiguration().isEmptyClusterAllowed();
	}

	public void setEmptyClusterAllowed(boolean emptyClusterAllowed)
	{
		this.getNestedConfiguration().setEmptyClusterAllowed(emptyClusterAllowed);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseClusterConfiguration#getTransactionIdentifierFactory()
	 */
	@Override
	public TransactionIdentifierFactory<? extends Object> getTransactionIdentifierFactory()
	{
		return (this.dispatcherFactory != null) ? new UUIDTransactionIdentifierFactory() : new SimpleTransactionIdentifierFactory();
	}

	@XmlType(name = "abstractNestedConfiguration")
	protected static abstract class NestedConfiguration<Z, D extends Database<Z>> implements DatabaseClusterConfiguration<Z, D>, DatabaseFactory<Z, D>
	{
		private static final long serialVersionUID = -5674156614205147546L;

		@XmlJavaTypeAdapter(BalancerFactoryAdapter.class)
		@XmlAttribute(name = "balancer")
		private BalancerFactory balancerFactory = ServiceLoaders.findService(BalancerFactory.class);
		
		@XmlJavaTypeAdapter(DatabaseMetaDataCacheFactoryAdapter.class)
		@XmlAttribute(name = "meta-data-cache")
		private DatabaseMetaDataCacheFactory databaseMetaDataCacheFactory = ServiceLoaders.findService(DatabaseMetaDataCacheFactory.class);
		
		@XmlJavaTypeAdapter(DialectFactoryAdapter.class)
		@XmlAttribute(name = "dialect")
		private DialectFactory dialectFactory = ServiceLoaders.findService(DialectFactory.class);
		
		@XmlJavaTypeAdapter(DurabilityFactoryAdapter.class)
		@XmlAttribute(name = "durability")
		private DurabilityFactory durabilityFactory = ServiceLoaders.findService(DurabilityFactory.class);

		@XmlJavaTypeAdapter(SinkSourceProviderAdapter.class)
		@XmlAttribute(name = "input-sink")
		private InputSinkProvider sinkSourceProvider = ServiceLoaders.findService(InputSinkProvider.class);
		
		private ExecutorServiceProvider executorProvider = new DefaultExecutorServiceProvider();
		private ThreadFactory threadFactory = Executors.defaultThreadFactory();
		private DecoderFactory decoderFactory = new MultiplexingDecoderFactory();
		private MBeanRegistrar<Z, D> registrar = new DefaultMBeanRegistrar<>();
		
		@XmlJavaTypeAdapter(TransactionModeAdapter.class)
		@XmlAttribute(name = "transaction-mode")
		private TransactionMode transactionMode = TransactionModeEnum.SERIAL;

		@XmlJavaTypeAdapter(CronExpressionAdapter.class)
		@XmlAttribute(name = "auto-activate-schedule")
		private CronExpression autoActivationExpression;
		@XmlJavaTypeAdapter(CronExpressionAdapter.class)
		@XmlAttribute(name = "failure-detect-schedule")
		private CronExpression failureDetectionExpression;
		
		@XmlAttribute(name = "eval-current-date")
		private Boolean currentDateEvaluationEnabled = false;
		@XmlAttribute(name = "eval-current-time")
		private Boolean currentTimeEvaluationEnabled = false;
		@XmlAttribute(name = "eval-current-timestamp")
		private Boolean currentTimestampEvaluationEnabled = false;
		@XmlAttribute(name = "eval-rand")
		private Boolean randEvaluationEnabled = false;
		
		@XmlAttribute(name = "detect-identity-columns")
		private Boolean identityColumnDetectionEnabled = false;
		@XmlAttribute(name = "detect-sequences")
		private Boolean sequenceDetectionEnabled = false;

		@XmlAttribute(name = "allow-empty-cluster")
		private Boolean emptyClusterAllowed = false;
		
		private String defaultSynchronizationStrategy;
		
		private ConcurrentMap<String, D> databases = new ConcurrentHashMap<>();
		
		@Override
		public DatabaseFactory<Z, D> getDatabaseFactory()
		{
			return this;
		}

		@XmlIDREF
		@XmlAttribute(name = "default-sync", required = true)
		private SynchronizationStrategyDescriptor getDefaultSynchronizationStrategyDescriptor()
		{
			SynchronizationStrategyDescriptor descriptor = new SynchronizationStrategyDescriptor();
			descriptor.setId(this.defaultSynchronizationStrategy);
			return descriptor;
		}
		
		@SuppressWarnings("unused")
		private void setDefaultSynchronizationStrategyDescriptor(SynchronizationStrategyDescriptor descriptor)
		{
			this.defaultSynchronizationStrategy = descriptor.getId();
		}
		
		@Override
		public ConcurrentMap<String, D> getDatabaseMap()
		{
			return this.databases;
		}
		
		@Override
		public CronExpression getAutoActivationExpression()
		{
			return this.autoActivationExpression;
		}

		void setAutoActivationExpression(CronExpression expression)
		{
			this.autoActivationExpression = expression;
		}
		
		@Override
		public BalancerFactory getBalancerFactory()
		{
			return this.balancerFactory;
		}

		void setBalancerFactory(BalancerFactory factory)
		{
			this.balancerFactory = factory;
		}
		
		@Override
		public CommandDispatcherFactory getDispatcherFactory()
		{
			throw new IllegalStateException();
		}

		@Override
		public DatabaseMetaDataCacheFactory getDatabaseMetaDataCacheFactory()
		{
			return this.databaseMetaDataCacheFactory;
		}

		void setDatabaseMetaDataCacheFactory(DatabaseMetaDataCacheFactory factory)
		{
			this.databaseMetaDataCacheFactory = factory;
		}
		
		@Override
		public String getDefaultSynchronizationStrategy()
		{
			return this.defaultSynchronizationStrategy;
		}

		void setDefaultSynchronizationStrategy(String strategy)
		{
			this.defaultSynchronizationStrategy = strategy;
		}
		
		@Override
		public DialectFactory getDialectFactory()
		{
			return this.dialectFactory;
		}

		void setDialectFactory(DialectFactory factory)
		{
			this.dialectFactory = factory;
		}
		
		@Override
		public DurabilityFactory getDurabilityFactory()
		{
			return this.durabilityFactory;
		}

		void setDurabilityFactory(DurabilityFactory factory)
		{
			this.durabilityFactory = factory;
		}
		
		@Override
		public ExecutorServiceProvider getExecutorProvider()
		{
			return this.executorProvider;
		}

		void setExecutorProvider(ExecutorServiceProvider provider)
		{
			this.executorProvider = provider;
		}
		
		@Override
		public ThreadFactory getThreadFactory()
		{
			return this.threadFactory;
		}

		void setThreadFactory(ThreadFactory factory)
		{
			this.threadFactory = factory;
		}
		
		@Override
		public DecoderFactory getDecoderFactory()
		{
			return this.decoderFactory;
		}

		void setDecoderFactory(DecoderFactory factory)
		{
			this.decoderFactory = factory;
		}
		
		@Override
		public MBeanRegistrar<Z, D> getMBeanRegistrar()
		{
			return this.registrar;
		}
		
		void setMBeanRegistrar(MBeanRegistrar<Z, D> registrar)
		{
			this.registrar = registrar;
		}
		
		@Override
		public CronExpression getFailureDetectionExpression()
		{
			return this.failureDetectionExpression;
		}

		void setFailureDetectionExpression(CronExpression expression)
		{
			this.failureDetectionExpression = expression;
		}
		
		@Override
		public StateManagerFactory getStateManagerFactory()
		{
			throw new IllegalStateException();
		}
		
		@Override
		public LockManagerFactory getLockManagerFactory()
		{
			throw new IllegalStateException();
		}

		@Override
		public Map<String, SynchronizationStrategy> getSynchronizationStrategyMap()
		{
			throw new IllegalStateException();
		}
		
		@Override
		public TransactionMode getTransactionMode()
		{
			return this.transactionMode;
		}

		void setTransactionMode(TransactionMode mode)
		{
			this.transactionMode = mode;
		}
		
		@Override
		public InputSinkProvider getInputSinkProvider()
		{
			return this.sinkSourceProvider;
		}

		void setSinkSourceProvider(InputSinkProvider provider)
		{
			this.sinkSourceProvider = provider;
		}
		
		@Override
		public boolean isCurrentDateEvaluationEnabled()
		{
			return this.currentDateEvaluationEnabled;
		}
		
		void setCurrentDateEvaluationEnabled(boolean enabled)
		{
			this.currentDateEvaluationEnabled = enabled;
		}
		
		@Override
		public boolean isCurrentTimeEvaluationEnabled()
		{
			return this.currentTimeEvaluationEnabled;
		}
		
		void setCurrentTimeEvaluationEnabled(boolean enabled)
		{
			this.currentTimeEvaluationEnabled = enabled;
		}
		
		@Override
		public boolean isCurrentTimestampEvaluationEnabled()
		{
			return this.currentTimestampEvaluationEnabled;
		}
		
		void setCurrentTimestampEvaluationEnabled(boolean enabled)
		{
			this.currentTimestampEvaluationEnabled = enabled;
		}
		
		@Override
		public boolean isIdentityColumnDetectionEnabled()
		{
			return this.identityColumnDetectionEnabled;
		}
		
		void setIdentityColumnDetectionEnabled(boolean enabled)
		{
			this.identityColumnDetectionEnabled = enabled;
		}
		
		@Override
		public boolean isRandEvaluationEnabled()
		{
			return this.randEvaluationEnabled;
		}

		void setRandEvaluationEnabled(boolean enabled)
		{
			this.randEvaluationEnabled = enabled;
		}
		
		@Override
		public boolean isSequenceDetectionEnabled()
		{
			return this.sequenceDetectionEnabled;
		}
		
		void setSequenceDetectionEnabled(boolean enabled)
		{
			this.sequenceDetectionEnabled = enabled;
		}

		@Override
		public TransactionIdentifierFactory<? extends Object> getTransactionIdentifierFactory()
		{
			throw new IllegalStateException();
		}

		/* (non-Javadoc)
		 * @see net.sf.hajdbc.DatabaseClusterConfiguration#isEmptyClusterAllowed()
		 */
		@Override
		public boolean isEmptyClusterAllowed()
		{
			return this.emptyClusterAllowed;
		}
		
		void setEmptyClusterAllowed(boolean emptyClusterAllowed)
		{
			this.emptyClusterAllowed = emptyClusterAllowed;
		}
	}

	static class IdentifiableServiceAdapter<T extends Identifiable> extends XmlAdapter<String, T>
	{
		private final Class<T> serviceClass;
		
		IdentifiableServiceAdapter(Class<T> serviceClass)
		{
			this.serviceClass = serviceClass;
		}

		@Override
		public T unmarshal(final String value)
		{
			return ServiceLoaders.findRequiredService(new IdentifiableMatcher<T>(value), this.serviceClass);
		}

		@Override
		public String marshal(T service)
		{
			return service.getId();
		}
	}

	static class BalancerFactoryAdapter extends IdentifiableServiceAdapter<BalancerFactory>
	{
		BalancerFactoryAdapter()
		{
			super(BalancerFactory.class);
		}
	}

	static class DatabaseMetaDataCacheFactoryAdapter extends IdentifiableServiceAdapter<DatabaseMetaDataCacheFactory>
	{
		DatabaseMetaDataCacheFactoryAdapter()
		{
			super(DatabaseMetaDataCacheFactory.class);
		}
	}

	static class DurabilityFactoryAdapter extends IdentifiableServiceAdapter<DurabilityFactory>
	{
		DurabilityFactoryAdapter()
		{
			super(DurabilityFactory.class);
		}
	}

	static class TransactionModeAdapter extends EnumAdapter<TransactionMode, TransactionModeEnum>
	{
		@Override
		protected Class<TransactionModeEnum> getTargetClass()
		{
			return TransactionModeEnum.class;
		}
	}

	static abstract class EnumAdapter<I, E extends I> extends XmlAdapter<E, I>
	{
		@Override
		public I unmarshal(E enumerated)
		{
			return enumerated;
		}
		
		@Override
		public E marshal(I object)
		{
			return this.getTargetClass().cast(object);
		}

		protected abstract Class<E> getTargetClass();
	}

	static class DialectFactoryAdapter extends IdentifiableServiceAdapter<DialectFactory>
	{
		DialectFactoryAdapter()
		{
			super(DialectFactory.class);
		}
	}

	static class SinkSourceProviderAdapter extends IdentifiableServiceAdapter<InputSinkProvider>
	{
		SinkSourceProviderAdapter()
		{
			super(InputSinkProvider.class);
		}
	}
	
	static class CronExpressionAdapter extends XmlAdapter<String, CronExpression>
	{
		@Override
		public String marshal(CronExpression expression)
		{
			return (expression != null) ? expression.getCronExpression() : null;
		}

		@Override
		public CronExpression unmarshal(String value) throws Exception
		{
			return (value != null) ? new CronExpression(value) : null;
		}
	}
	
	@XmlType
	static class CommandDispatcherFactoryDescriptor extends IdentifiableServiceDescriptor
	{
		@XmlAttribute(name = "id", required = false)
		private String id = "jgroups";
		
		@Override
		public String getId()
		{
			return this.id;
		}
		
		@Override
		public void setId(String id)
		{
			this.id = id;
		}
	}

	static class CommandDispatcherFactoryDescriptorAdapter extends IdentifiableServiceDescriptorAdapter<CommandDispatcherFactory, CommandDispatcherFactoryDescriptor>
	{
		CommandDispatcherFactoryDescriptorAdapter()
		{
			super(CommandDispatcherFactory.class, CommandDispatcherFactoryDescriptor.class);
		}
	}
	
	@XmlType
	static class SynchronizationStrategyDescriptor extends IdentifiableServiceDescriptor
	{
		@XmlID
		@XmlAttribute(name = "id", required = true)
		private String id;
		
		@Override
		public String getId()
		{
			return this.id;
		}
		
		@Override
		public void setId(String id)
		{
			this.id = id;
		}
	}
	
	static class SynchronizationStrategyDescriptorAdapter extends IdentifiableServiceDescriptorAdapter<SynchronizationStrategy, SynchronizationStrategyDescriptor>
	{
		SynchronizationStrategyDescriptorAdapter()
		{
			super(SynchronizationStrategy.class, SynchronizationStrategyDescriptor.class);
		}
	}
	
	@XmlType
	static class StateManagerFactoryDescriptor extends IdentifiableServiceDescriptor
	{
		@XmlAttribute(name = "id", required = true)
		private String id;
		
		@Override
		public String getId()
		{
			return this.id;
		}
		
		@Override
		public void setId(String id)
		{
			this.id = id;
		}
	}

	static class StateManagerFactoryDescriptorAdapter extends IdentifiableServiceDescriptorAdapter<StateManagerFactory, StateManagerFactoryDescriptor>
	{
		StateManagerFactoryDescriptorAdapter()
		{
			super(StateManagerFactory.class, StateManagerFactoryDescriptor.class);
		}
	}
	
	@XmlType
	static class LockManagerFactoryDescriptor extends IdentifiableServiceDescriptor
	{
		@XmlAttribute(name = "id", required = true)
		private String id;
		
		@Override
		public String getId()
		{
			return this.id;
		}
		
		@Override
		public void setId(String id)
		{
			this.id = id;
		}
	}

	static class LockManagerFactoryDescriptorAdapter extends IdentifiableServiceDescriptorAdapter<LockManagerFactory, LockManagerFactoryDescriptor>
	{
		LockManagerFactoryDescriptorAdapter()
		{
			super(LockManagerFactory.class, LockManagerFactoryDescriptor.class);
		}
	}

	static abstract class IdentifiableServiceDescriptor implements Identifiable
	{
		@XmlElement(name = "property")
		private List<Property> properties;
		
		public List<Property> getProperties()
		{
			return this.properties;
		}

		public void setProperties(List<Property> properties)
		{
			this.properties = properties;
		}
		
		public abstract void setId(String id);
	}

	static class IdentifiableServiceDescriptorAdapter<T extends Identifiable, D extends IdentifiableServiceDescriptor> extends XmlAdapter<D, T>
	{
		private final Class<T> serviceClass;
		private final Class<D> descriptorClass;
		
		IdentifiableServiceDescriptorAdapter(Class<T> serviceClass, Class<D> descriptorClass)
		{
			this.serviceClass = serviceClass;
			this.descriptorClass = descriptorClass;
		}
		
		@Override
		public D marshal(T object) throws Exception
		{
			D result = this.descriptorClass.newInstance();
			List<Property> properties = new LinkedList<>();
			
			result.setId(object.getId());
			result.setProperties(properties);
			
			for (Map.Entry<PropertyDescriptor, PropertyEditor> entry: findDescriptors(object.getClass()).values())
			{
				PropertyDescriptor descriptor = entry.getKey();
				PropertyEditor editor = entry.getValue();
				
				Object value = descriptor.getReadMethod().invoke(object);
				if (value != null)
				{
					editor.setValue(value);
					
					Property property = new Property();
					property.setName(descriptor.getName());
					property.setValue(editor.getAsText());
					
					properties.add(property);
				}
			}
			
			return result;
		}

		@Override
		public T unmarshal(D target) throws Exception
		{
			T result = ServiceLoaders.findRequiredService(new IdentifiableMatcher<T>(target.getId()), this.serviceClass);
			List<Property> properties = target.getProperties();
			
			if (properties != null)
			{
				Map<String, Map.Entry<PropertyDescriptor, PropertyEditor>> descriptors = findDescriptors(result.getClass());
				
				for (Property property: properties)
				{
					String name = property.getName();
					Map.Entry<PropertyDescriptor, PropertyEditor> entry = descriptors.get(name);
					
					if (entry == null)
					{
						throw new IllegalArgumentException(Messages.INVALID_PROPERTY.getMessage(name, result.getClass().getName()));
					}
					
					PropertyDescriptor descriptor = entry.getKey();
					PropertyEditor editor = entry.getValue();

					String textValue = property.getValue();
					
					try
					{
						editor.setAsText(textValue);
					}
					catch (Exception e)
					{
						throw new IllegalArgumentException(Messages.INVALID_PROPERTY_VALUE.getMessage(textValue, name, result.getClass().getName()));
					}
					descriptor.getWriteMethod().invoke(result, editor.getValue());
				}
			}
			return result;
		}
	}

	@XmlType
	protected static class Property
	{
		@XmlAttribute(required = true)
		private String name;
		@XmlValue
		private String value;
		
		public String getName()
		{
			return this.name;
		}
		
		public void setName(String name)
		{
			this.name = name;
		}
		
		public String getValue()
		{
			return this.value;
		}
		
		public void setValue(String value)
		{
			this.value = value;
		}
	}
}
