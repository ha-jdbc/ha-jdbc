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
package net.sf.hajdbc.xml;

import java.io.StringReader;
import java.io.StringWriter;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Map;

import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import net.sf.hajdbc.DatabaseClusterConfiguration;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.balancer.BalancerFactoryEnum;
import net.sf.hajdbc.cache.DatabaseMetaDataCacheFactoryEnum;
import net.sf.hajdbc.dialect.DialectFactoryEnum;
import net.sf.hajdbc.durability.DurabilityFactoryEnum;
import net.sf.hajdbc.sql.DefaultExecutorServiceProvider;
import net.sf.hajdbc.sql.DriverDatabase;
import net.sf.hajdbc.sql.DriverDatabaseClusterConfiguration;
import net.sf.hajdbc.sql.TransactionModeEnum;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

public class XMLDatabaseClusterConfigurationFactoryTest
{
	@Test
	public void createConfiguration() throws SQLException
	{
		StringBuilder builder = new StringBuilder();
		builder.append("<?xml version=\"1.0\"?>");
		builder.append("<ha-jdbc xmlns=\"").append(SchemaGenerator.NAMESPACE).append("\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">");
		builder.append("\t<sync id=\"diff\" class=\"net.sf.hajdbc.sync.DifferentialSynchronizationStrategy\"><property name=\"fetchSize\">100</property><property name=\"maxBatchSize\">100</property></sync>");
		builder.append("\t<cluster default-sync=\"diff\">");
		builder.append("\t\t<database id=\"db1\">");
		builder.append("\t\t\t<name>jdbc:mock:db1</name>");
		builder.append("\t\t</database>");
		builder.append("\t\t<database id=\"db2\">");
		builder.append("\t\t\t<name>jdbc:mock:db2</name>");
		builder.append("\t\t</database>");
		builder.append("\t</cluster>");
		builder.append("</ha-jdbc>");
		
		String xml = builder.toString();
		
		XMLStreamFactory streamFactory = EasyMock.createStrictMock(XMLStreamFactory.class);
		
		XMLDatabaseClusterConfigurationFactory<Driver, DriverDatabase> factory = new XMLDatabaseClusterConfigurationFactory<Driver, DriverDatabase>(DriverDatabaseClusterConfiguration.class, streamFactory);
		
		EasyMock.expect(streamFactory.createSource()).andReturn(new StreamSource(new StringReader(xml)));
		
		EasyMock.replay(streamFactory);
		
		DatabaseClusterConfiguration<Driver, DriverDatabase> configuration = factory.createConfiguration();
		
		EasyMock.verify(streamFactory);
		
		Assert.assertNull(configuration.getDispatcherFactory());
		Map<String, SynchronizationStrategy> strategies = configuration.getSynchronizationStrategyMap();
		Assert.assertNotNull(strategies);
		Assert.assertEquals(1, strategies.size());
		
		SynchronizationStrategy strategy = strategies.get("diff");
		
		Assert.assertNotNull(strategy);
		
		Assert.assertSame(BalancerFactoryEnum.ROUND_ROBIN, configuration.getBalancerFactory());
	   Assert.assertSame(DatabaseMetaDataCacheFactoryEnum.EAGER, configuration.getDatabaseMetaDataCacheFactory());
	   Assert.assertEquals("diff", configuration.getDefaultSynchronizationStrategy());
	   Assert.assertSame(DialectFactoryEnum.STANDARD, configuration.getDialectFactory());
	   Assert.assertSame(DurabilityFactoryEnum.FINE, configuration.getDurabilityFactory());
	   Assert.assertSame(TransactionModeEnum.SERIAL, configuration.getTransactionMode());
	   
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
	   
	   EasyMock.reset(streamFactory);
	   
	   StringWriter writer = new StringWriter();
	   
	   EasyMock.expect(streamFactory.createResult()).andReturn(new StreamResult(writer));
	   
	   EasyMock.replay(streamFactory);
	   
		factory.export(configuration);
		
		EasyMock.verify(streamFactory);
		
		System.out.println(writer.toString());
	}
}
