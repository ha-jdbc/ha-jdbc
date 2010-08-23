/*
 * HA-JDBC: High-Availability JDBC
 * Copyright 2004-2009 Paul Ferraro
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.ExecutorServiceProvider;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.TransactionMode;
import net.sf.hajdbc.balancer.BalancerFactory;
import net.sf.hajdbc.balancer.BalancerFactoryEnum;
import net.sf.hajdbc.cache.DatabaseMetaDataCacheFactory;
import net.sf.hajdbc.cache.DatabaseMetaDataCacheFactoryEnum;
import net.sf.hajdbc.codec.CodecFactory;
import net.sf.hajdbc.codec.simple.SimpleCodecFactory;
import net.sf.hajdbc.dialect.CustomDialectFactory;
import net.sf.hajdbc.dialect.DialectFactory;
import net.sf.hajdbc.dialect.DialectFactoryEnum;
import net.sf.hajdbc.distributed.CommandDispatcherFactory;
import net.sf.hajdbc.distributed.jgroups.DefaultChannelProvider;
import net.sf.hajdbc.durability.DurabilityFactory;
import net.sf.hajdbc.durability.DurabilityFactoryEnum;
import net.sf.hajdbc.state.StateManagerProvider;
import net.sf.hajdbc.state.sql.SQLStateManagerProvider;

import org.quartz.CronExpression;

/**
 * @author paul
 *
 */
@XmlType(propOrder = { "dispatcherFactory", "synchronizationStrategyDescriptors" })
public abstract class AbstractDatabaseClusterConfiguration<Z, D extends Database<Z>> implements DatabaseClusterConfiguration<Z, D>
{
	private static final long serialVersionUID = -2808296483725374829L;

	@XmlElement(name = "distributable", type = DefaultChannelProvider.class)
	private CommandDispatcherFactory dispatcherFactory;
	
	Map<String, SynchronizationStrategy> synchronizationStrategies = new HashMap<String, SynchronizationStrategy>();
	
	protected abstract NestedConfiguration<Z, D> getNestedConfiguration();
	
	@SuppressWarnings("unused")
	@XmlElement(name = "sync")
	private SynchronizationStrategyDescriptor[] getSynchronizationStrategyDescriptors() throws Exception
	{
		List<SynchronizationStrategyDescriptor> results = new ArrayList<SynchronizationStrategyDescriptor>(this.synchronizationStrategies.size());
		
		for (Map.Entry<String, SynchronizationStrategy> entry: this.synchronizationStrategies.entrySet())
		{
			SynchronizationStrategyDescriptor result = new SynchronizationStrategyDescriptor();
			
			SynchronizationStrategy strategy = entry.getValue();				
			Class<? extends SynchronizationStrategy> targetClass = strategy.getClass();
			
			result.setId(entry.getKey());
			result.setTargetClass(targetClass);
			
			for (PropertyDescriptor descriptor: this.findDescriptors(targetClass).values())
			{
				Property property = new Property();					
				PropertyEditor editor = PropertyEditorManager.findEditor(descriptor.getPropertyType());
				
				if (editor == null) continue;
				
				editor.setValue(descriptor.getReadMethod().invoke(strategy));
				
				property.setName(descriptor.getName());
				property.setValue(editor.getAsText());
			}
			
			results.add(result);
		}
		
		return results.toArray(new SynchronizationStrategyDescriptor[results.size()]);
	}
	
	@SuppressWarnings("unused")
	private void setSynchronizationStrategyDescriptors(SynchronizationStrategyDescriptor[] entries) throws Exception
	{
		for (SynchronizationStrategyDescriptor entry: entries)
		{
			Class<? extends SynchronizationStrategy> targetClass = entry.getTargetClass();
			SynchronizationStrategy strategy = targetClass.newInstance();
			
			if (entry.getProperties() != null)
			{
				Map<String, PropertyDescriptor> descriptors = this.findDescriptors(targetClass);
				
				for (Property property: entry.getProperties())
				{
					String name = property.getName();
					PropertyDescriptor descriptor = descriptors.get(name);
					
					if (descriptor == null)
					{
						// not found
					}
					
					PropertyEditor editor = PropertyEditorManager.findEditor(descriptor.getPropertyType());
					String textValue = property.getValue();
					
					try
					{
						if (editor == null)
						{
							throw new Exception();
						}

						editor.setAsText(textValue);
					}
					catch (Exception e)
					{
						throw new IllegalArgumentException(Messages.INVALID_PROPERTY_VALUE.getMessage(textValue, name, targetClass.getName()));
					}
					
					descriptor.getWriteMethod().invoke(strategy, editor.getValue());
				}
			}
			
			this.synchronizationStrategies.put(entry.getId(), strategy);
		}
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
	public Map<String, D> getDatabaseMap()
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
	 * @see net.sf.hajdbc.DatabaseClusterConfiguration#getCodecFactory()
	 */
	@Override
	public CodecFactory getCodecFactory()
	{
		return this.getNestedConfiguration().getCodecFactory();
	}

	public void setCodecFactory(CodecFactory factory)
	{
		this.getNestedConfiguration().setCodecFactory(factory);
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
	 * @see net.sf.hajdbc.DatabaseClusterConfiguration#getStateManagerProvider()
	 */
	@Override
	public StateManagerProvider getStateManagerProvider()
	{
		return this.getNestedConfiguration().getStateManagerProvider();
	}

	public void setStateManagerProvider(StateManagerProvider provider)
	{
		this.getNestedConfiguration().setStateManagerProvider(provider);
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
	
	private Map<String, PropertyDescriptor> findDescriptors(Class<?> targetClass) throws Exception
	{
		Map<String, PropertyDescriptor> map = new HashMap<String, PropertyDescriptor>();
		
		for (PropertyDescriptor descriptor: Introspector.getBeanInfo(targetClass).getPropertyDescriptors())
		{
			// Prevent Object.getClass() from being read as a property
			if (descriptor.getName().equals("class")) continue;
			
			map.put(descriptor.getName(), descriptor);
		}
		
		return map;
	}
	
	@XmlType(name = "abstractNestedConfiguration")
	protected static abstract class NestedConfiguration<Z, D extends Database<Z>> implements DatabaseClusterConfiguration<Z, D>
	{
		private static final long serialVersionUID = -5674156614205147546L;

		@XmlJavaTypeAdapter(BalancerFactoryAdapter.class)
		@XmlAttribute(name = "balancer")
		private BalancerFactory balancerFactory = BalancerFactoryEnum.ROUND_ROBIN;
		
		@XmlJavaTypeAdapter(DatabaseMetaDataCacheFactoryAdapter.class)
		@XmlAttribute(name = "meta-data-cache")
		private DatabaseMetaDataCacheFactory databaseMetaDataCacheFactory = DatabaseMetaDataCacheFactoryEnum.EAGER;
		
		@XmlJavaTypeAdapter(DialectFactoryAdapter.class)
		@XmlAttribute(name = "dialect")
		private DialectFactory dialectFactory = DialectFactoryEnum.STANDARD;
		
		@XmlJavaTypeAdapter(DurabilityFactoryAdapter.class)
		@XmlAttribute(name = "durability")
		private DurabilityFactory durabilityFactory = DurabilityFactoryEnum.FINE;

		private ExecutorServiceProvider executorProvider = new DefaultExecutorServiceProvider();
		private ThreadFactory threadFactory = Executors.defaultThreadFactory();
		private CodecFactory codecFactory = new SimpleCodecFactory();
		private StateManagerProvider stateManagerProvider = new SQLStateManagerProvider();

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

		private String defaultSynchronizationStrategy;
		
		private Map<String, D> databases = new HashMap<String, D>();
		
		@SuppressWarnings("unused")
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
		public Map<String, D> getDatabaseMap()
		{
			return this.databases;
		}
		
		@Override
		public CronExpression getAutoActivationExpression()
		{
			return this.autoActivationExpression;
		}

		public void setAutoActivationExpression(CronExpression expression)
		{
			this.autoActivationExpression = expression;
		}
		
		@Override
		public BalancerFactory getBalancerFactory()
		{
			return this.balancerFactory;
		}

		public void setBalancerFactory(BalancerFactory factory)
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

		public void setDatabaseMetaDataCacheFactory(DatabaseMetaDataCacheFactory factory)
		{
			this.databaseMetaDataCacheFactory = factory;
		}
		
		@Override
		public String getDefaultSynchronizationStrategy()
		{
			return this.defaultSynchronizationStrategy;
		}

		public void setDefaultSynchronizationStrategy(String strategy)
		{
			this.defaultSynchronizationStrategy = strategy;
		}
		
		@Override
		public DialectFactory getDialectFactory()
		{
			return this.dialectFactory;
		}

		public void setDialectFactory(DialectFactory factory)
		{
			this.dialectFactory = factory;
		}
		
		@Override
		public DurabilityFactory getDurabilityFactory()
		{
			return this.durabilityFactory;
		}

		public void setDurabilityFactory(DurabilityFactory factory)
		{
			this.durabilityFactory = factory;
		}
		
		@Override
		public ExecutorServiceProvider getExecutorProvider()
		{
			return this.executorProvider;
		}

		public void setExecutorProvider(ExecutorServiceProvider provider)
		{
			this.executorProvider = provider;
		}
		
		@Override
		public ThreadFactory getThreadFactory()
		{
			return this.threadFactory;
		}

		public void setThreadFactory(ThreadFactory factory)
		{
			this.threadFactory = factory;
		}
		
		@Override
		public CodecFactory getCodecFactory()
		{
			return this.codecFactory;
		}

		public void setCodecFactory(CodecFactory factory)
		{
			this.codecFactory = factory;
		}
		
		@Override
		public CronExpression getFailureDetectionExpression()
		{
			return this.failureDetectionExpression;
		}

		public void setFailureDetectionExpression(CronExpression expression)
		{
			this.failureDetectionExpression = expression;
		}
		
		@Override
		public StateManagerProvider getStateManagerProvider()
		{
			return this.stateManagerProvider;
		}

		public void setStateManagerProvider(StateManagerProvider provider)
		{
			this.stateManagerProvider = provider;
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

		public void setTransactionMode(TransactionMode mode)
		{
			this.transactionMode = mode;
		}
		
		@Override
		public boolean isCurrentDateEvaluationEnabled()
		{
			return this.currentDateEvaluationEnabled;
		}
		
		public void setCurrentDateEvaluationEnabled(boolean enabled)
		{
			this.currentDateEvaluationEnabled = enabled;
		}
		
		@Override
		public boolean isCurrentTimeEvaluationEnabled()
		{
			return this.currentTimeEvaluationEnabled;
		}
		
		public void setCurrentTimeEvaluationEnabled(boolean enabled)
		{
			this.currentTimeEvaluationEnabled = enabled;
		}
		
		@Override
		public boolean isCurrentTimestampEvaluationEnabled()
		{
			return this.currentTimestampEvaluationEnabled;
		}
		
		public void setCurrentTimestampEvaluationEnabled(boolean enabled)
		{
			this.currentTimestampEvaluationEnabled = enabled;
		}
		
		@Override
		public boolean isIdentityColumnDetectionEnabled()
		{
			return this.identityColumnDetectionEnabled;
		}
		
		public void setIdentityColumnDetectionEnabled(boolean enabled)
		{
			this.identityColumnDetectionEnabled = enabled;
		}
		
		@Override
		public boolean isRandEvaluationEnabled()
		{
			return this.randEvaluationEnabled;
		}

		public void setRandEvaluationEnabled(boolean enabled)
		{
			this.randEvaluationEnabled = enabled;
		}
		
		@Override
		public boolean isSequenceDetectionEnabled()
		{
			return this.sequenceDetectionEnabled;
		}
		
		public void setSequenceDetectionEnabled(boolean enabled)
		{
			this.sequenceDetectionEnabled = enabled;
		}
	}

	static class BalancerFactoryAdapter extends EnumAdapter<BalancerFactory, BalancerFactoryEnum>
	{
		@Override
		protected Class<BalancerFactoryEnum> getTargetClass()
		{
			return BalancerFactoryEnum.class;
		}
	}

	static class DatabaseMetaDataCacheFactoryAdapter extends EnumAdapter<DatabaseMetaDataCacheFactory, DatabaseMetaDataCacheFactoryEnum>
	{
		@Override
		protected Class<DatabaseMetaDataCacheFactoryEnum> getTargetClass()
		{
			return DatabaseMetaDataCacheFactoryEnum.class;
		}
	}

	static class DurabilityFactoryAdapter extends EnumAdapter<DurabilityFactory, DurabilityFactoryEnum>
	{
		@Override
		protected Class<DurabilityFactoryEnum> getTargetClass()
		{
			return DurabilityFactoryEnum.class;
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
		public I unmarshal(E enumerated) throws Exception
		{
			return enumerated;
		}
		
		@Override
		public E marshal(I object) throws Exception
		{
			return this.getTargetClass().cast(object);
		}

		protected abstract Class<E> getTargetClass();		
	}

	static class DialectFactoryAdapter extends XmlAdapter<String, DialectFactory>
	{
		@Override
		public String marshal(DialectFactory factory)
		{
			return factory.toString();
		}

		@Override
		public DialectFactory unmarshal(String value) throws Exception
		{
			try
			{
				return DialectFactoryEnum.valueOf(value.toUpperCase());
			}
			catch (IllegalArgumentException e)
			{
				return new CustomDialectFactory(Class.forName(value).asSubclass(Dialect.class));
			}
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
	static class SynchronizationStrategyDescriptor
	{
		@XmlID
		@XmlAttribute(name = "id", required = true)
		private String id;
		
		@XmlAttribute(name = "class", required = true)
		private Class<? extends SynchronizationStrategy> targetClass;
		
		@XmlElement(name = "property")
		private List<Property> properties;

		public String getId()
		{
			return this.id;
		}
		
		public void setId(String id)
		{
			this.id = id;
		}
		
		public Class<? extends SynchronizationStrategy> getTargetClass()
		{
			return this.targetClass;
		}
		
		public void setTargetClass(Class<? extends SynchronizationStrategy> targetClass)
		{
			this.targetClass = targetClass;
		}
		
		public List<Property> getProperties()
		{
			return this.properties;
		}

		public void setProperties(List<Property> properties)
		{
			this.properties = properties;
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
