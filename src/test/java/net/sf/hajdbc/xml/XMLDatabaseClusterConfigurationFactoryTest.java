package net.sf.hajdbc.xml;

import java.sql.SQLException;
import java.util.Map;

import junit.framework.Assert;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.balancer.BalancerFactoryEnum;
import net.sf.hajdbc.cache.DatabaseMetaDataCacheFactoryEnum;
import net.sf.hajdbc.dialect.DialectFactoryEnum;
import net.sf.hajdbc.durability.DurabilityFactoryEnum;
import net.sf.hajdbc.sql.DefaultExecutorServiceProvider;
import net.sf.hajdbc.sql.DriverDatabase;
import net.sf.hajdbc.sql.DriverDatabaseClusterConfiguration;
import net.sf.hajdbc.sql.TransactionMode;

import org.junit.Before;
import org.junit.Test;

public class XMLDatabaseClusterConfigurationFactoryTest
{
	private XMLDatabaseClusterConfigurationFactory factory;
	
	@Before
	public void init()
	{
		this.factory = new XMLDatabaseClusterConfigurationFactory("test-database-cluster", null);
	}
	
	@Test
	public void createConfiguration() throws SQLException
	{
		DriverDatabaseClusterConfiguration configuration = this.factory.createConfiguration(DriverDatabaseClusterConfiguration.class);
		
		Assert.assertNull(configuration.getChannelProvider());
		Map<String, SynchronizationStrategy> strategies = configuration.getSynchronizationStrategyMap();
		Assert.assertNotNull(strategies);
		Assert.assertEquals(1, strategies.size());
		
		SynchronizationStrategy strategy = strategies.get("passive");
		
		Assert.assertNotNull(strategy);
		
		Assert.assertSame(BalancerFactoryEnum.ROUND_ROBIN, configuration.getBalancerFactory());
	   Assert.assertSame(DatabaseMetaDataCacheFactoryEnum.EAGER, configuration.getDatabaseMetaDataCacheFactory());
	   Assert.assertEquals("passive", configuration.getDefaultSynchronizationStrategy());
	   Assert.assertSame(DialectFactoryEnum.STANDARD, configuration.getDialectFactory());
	   Assert.assertSame(DurabilityFactoryEnum.FINE, configuration.getDurabilityFactory());
	   Assert.assertSame(TransactionMode.SERIAL, configuration.getTransactionMode());
	   
	   DefaultExecutorServiceProvider executorProvider = (DefaultExecutorServiceProvider) configuration.getExecutorProvider();
	   
	   Assert.assertNotNull(executorProvider);
	   Assert.assertEquals(60, executorProvider.getMaxIdle());
	   Assert.assertEquals(100, executorProvider.getMaxThreads());
	   Assert.assertEquals(0, executorProvider.getMinThreads());
	   
	   Assert.assertNull(configuration.getAutoActivationExpression());
	   Assert.assertNull(configuration.getFailureDetectionExpression());
	   
	   Assert.assertFalse(configuration.isCurrentDateEvaluationEnabled());
	   Assert.assertFalse(configuration.isCurrentTimeEvaluationEnabled());
	   Assert.assertFalse(configuration.isCurrentTimestampEvaluationEnabled());
	   Assert.assertFalse(configuration.isIdentityColumnDetectionEnabled());
	   Assert.assertFalse(configuration.isRandEvaluationEnabled());
	   Assert.assertFalse(configuration.isSequenceDetectionEnabled());
	   
	   Map<String, DriverDatabase> databases = configuration.getDatabaseMap();
	   
	   Assert.assertNotNull(databases);
	   Assert.assertEquals(2, databases.size());
	   
	   DriverDatabase db1 = databases.get("db1");
	   
	   Assert.assertNotNull(db1);
	   Assert.assertEquals("db1", db1.getId());
	   Assert.assertEquals("jdbc:mock:db1", db1.getName());
	   Assert.assertEquals(1, db1.getWeight());
	   Assert.assertFalse(db1.isLocal());
	   Assert.assertFalse(db1.isActive());
	   Assert.assertFalse(db1.isDirty());
	   
	   DriverDatabase db2 = databases.get("db2");
	   
	   Assert.assertNotNull(db2);
	   Assert.assertEquals("db2", db2.getId());
	   Assert.assertEquals("jdbc:mock:db2", db2.getName());
	   Assert.assertEquals(1, db2.getWeight());
	   Assert.assertFalse(db2.isLocal());
	   Assert.assertFalse(db2.isActive());
	   Assert.assertFalse(db2.isDirty());
	   
		this.factory.added(null, configuration);
	}
}
