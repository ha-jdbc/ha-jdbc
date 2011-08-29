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

import static org.junit.Assert.*;
import org.junit.Test;
import static org.mockito.Mockito.*;

public class XMLDatabaseClusterConfigurationFactoryTest
{
	@Test
	public void createConfiguration() throws SQLException
	{
		StringBuilder builder = new StringBuilder();
		builder.append("<?xml version=\"1.0\"?>");
		builder.append("<ha-jdbc xmlns=\"").append(SchemaGenerator.NAMESPACE).append("\">");
		builder.append("\t<sync id=\"diff\" class=\"net.sf.hajdbc.sync.DifferentialSynchronizationStrategy\"><property name=\"fetchSize\">100</property><property name=\"maxBatchSize\">100</property></sync>");
		builder.append("\t<state class=\"net.sf.hajdbc.state.sql.SQLStateManagerFactory\"><property name=\"urlPattern\">jdbc:h2:{0}</property></state>");
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
		
		XMLStreamFactory streamFactory = mock(XMLStreamFactory.class);
		
		XMLDatabaseClusterConfigurationFactory<Driver, DriverDatabase> factory = new XMLDatabaseClusterConfigurationFactory<Driver, DriverDatabase>(DriverDatabaseClusterConfiguration.class, streamFactory);
		
		when(streamFactory.createSource()).thenReturn(new StreamSource(new StringReader(xml)));
		
		DatabaseClusterConfiguration<Driver, DriverDatabase> configuration = factory.createConfiguration();
		
		assertNull(configuration.getDispatcherFactory());
		Map<String, SynchronizationStrategy> strategies = configuration.getSynchronizationStrategyMap();
		assertNotNull(strategies);
		assertEquals(1, strategies.size());
		
		SynchronizationStrategy strategy = strategies.get("diff");
		
		assertNotNull(strategy);
		
		assertSame(BalancerFactoryEnum.ROUND_ROBIN, configuration.getBalancerFactory());
	   assertSame(DatabaseMetaDataCacheFactoryEnum.EAGER, configuration.getDatabaseMetaDataCacheFactory());
	   assertEquals("diff", configuration.getDefaultSynchronizationStrategy());
	   assertSame(DialectFactoryEnum.STANDARD, configuration.getDialectFactory());
	   assertSame(DurabilityFactoryEnum.FINE, configuration.getDurabilityFactory());
	   assertSame(TransactionModeEnum.SERIAL, configuration.getTransactionMode());
	   
	   DefaultExecutorServiceProvider executorProvider = (DefaultExecutorServiceProvider) configuration.getExecutorProvider();
	   
	   assertNotNull(executorProvider);
	   assertEquals(60, executorProvider.getMaxIdle());
	   assertEquals(100, executorProvider.getMaxThreads());
	   assertEquals(0, executorProvider.getMinThreads());
	   
	   assertNull(configuration.getAutoActivationExpression());
	   assertNull(configuration.getFailureDetectionExpression());
	   
	   assertFalse(configuration.isCurrentDateEvaluationEnabled());
	   assertFalse(configuration.isCurrentTimeEvaluationEnabled());
	   assertFalse(configuration.isCurrentTimestampEvaluationEnabled());
	   assertFalse(configuration.isIdentityColumnDetectionEnabled());
	   assertFalse(configuration.isRandEvaluationEnabled());
	   assertFalse(configuration.isSequenceDetectionEnabled());
	   
	   Map<String, DriverDatabase> databases = configuration.getDatabaseMap();
	   
	   assertNotNull(databases);
	   assertEquals(2, databases.size());
	   
	   DriverDatabase db1 = databases.get("db1");
	   
	   assertNotNull(db1);
	   assertEquals("db1", db1.getId());
	   assertEquals("jdbc:mock:db1", db1.getName());
	   assertEquals(1, db1.getWeight());
	   assertFalse(db1.isLocal());
	   assertFalse(db1.isActive());
	   assertFalse(db1.isDirty());
	   
	   DriverDatabase db2 = databases.get("db2");
	   
	   assertNotNull(db2);
	   assertEquals("db2", db2.getId());
	   assertEquals("jdbc:mock:db2", db2.getName());
	   assertEquals(1, db2.getWeight());
	   assertFalse(db2.isLocal());
	   assertFalse(db2.isActive());
	   assertFalse(db2.isDirty());
	   
	   reset(streamFactory);
	   
	   StringWriter writer = new StringWriter();
	   
	   when(streamFactory.createResult()).thenReturn(new StreamResult(writer));
	   
		factory.export(configuration);
		
		System.out.println(writer.toString());
	}
}
