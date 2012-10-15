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
import net.sf.hajdbc.balancer.load.LoadBalancerFactory;
import net.sf.hajdbc.cache.eager.EagerDatabaseMetaDataCacheFactory;
import net.sf.hajdbc.dialect.StandardDialectFactory;
import net.sf.hajdbc.durability.fine.FineDurabilityFactory;
import net.sf.hajdbc.sql.DriverDatabase;
import net.sf.hajdbc.sql.DriverDatabaseClusterConfiguration;
import net.sf.hajdbc.sql.TransactionModeEnum;
import net.sf.hajdbc.state.StateManagerFactory;
import net.sf.hajdbc.state.sql.SQLStateManagerFactory;
import net.sf.hajdbc.sync.DifferentialSynchronizationStrategy;

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
		builder.append("\t<sync id=\"diff\"><property name=\"fetchSize\">100</property><property name=\"maxBatchSize\">100</property></sync>");
		builder.append("\t<state id=\"sql\"><property name=\"urlPattern\">jdbc:h2:{0}</property><property name=\"minIdle\">1</property></state>");
		builder.append("\t<cluster default-sync=\"diff\">");
		builder.append("\t\t<database id=\"db1\" location=\"jdbc:mock:db1\"/>");
		builder.append("\t\t<database id=\"db2\" location=\"jdbc:mock:db2\"/>");
		builder.append("\t</cluster>");
		builder.append("</ha-jdbc>");
		
		String xml = builder.toString();
		
		XMLStreamFactory streamFactory = mock(XMLStreamFactory.class);
		
		XMLDatabaseClusterConfigurationFactory<Driver, DriverDatabase> factory = new XMLDatabaseClusterConfigurationFactory<Driver, DriverDatabase>(DriverDatabaseClusterConfiguration.class, streamFactory);
		
		when(streamFactory.createSource()).thenReturn(new StreamSource(new StringReader(xml)));
		
		DatabaseClusterConfiguration<Driver, DriverDatabase> configuration = factory.createConfiguration();
		
		assertNull(configuration.getDispatcherFactory());
		Map<String, SynchronizationStrategy> syncStrategies = configuration.getSynchronizationStrategyMap();
		assertNotNull(syncStrategies);
		assertEquals(1, syncStrategies.size());
		
		SynchronizationStrategy syncStrategy = syncStrategies.get("diff");
		
		assertNotNull(syncStrategy);
		assertTrue(syncStrategy instanceof DifferentialSynchronizationStrategy);
		DifferentialSynchronizationStrategy diffStrategy = (DifferentialSynchronizationStrategy) syncStrategy;
		assertEquals(100, diffStrategy.getFetchSize());
		assertEquals(100, diffStrategy.getMaxBatchSize());
		assertNull(diffStrategy.getVersionPattern());
		
		StateManagerFactory stateManagerFactory = configuration.getStateManagerFactory();
		assertTrue(stateManagerFactory instanceof SQLStateManagerFactory);
		SQLStateManagerFactory sqlStateManagerFactory = (SQLStateManagerFactory) stateManagerFactory;
		assertEquals("jdbc:h2:{0}", sqlStateManagerFactory.getUrlPattern());
		assertNull(sqlStateManagerFactory.getUser());
		assertNull(sqlStateManagerFactory.getPassword());
		
		assertEquals(LoadBalancerFactory.class, configuration.getBalancerFactory().getClass());
		assertEquals(EagerDatabaseMetaDataCacheFactory.class, configuration.getDatabaseMetaDataCacheFactory().getClass());
		assertEquals("diff", configuration.getDefaultSynchronizationStrategy());
		assertEquals(StandardDialectFactory.class, configuration.getDialectFactory().getClass());
		assertEquals(FineDurabilityFactory.class, configuration.getDurabilityFactory().getClass());
		assertSame(TransactionModeEnum.SERIAL, configuration.getTransactionMode());
		
		assertNotNull(configuration.getExecutorProvider());
		
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
		assertEquals("jdbc:mock:db1", db1.getLocation());
		assertEquals(1, db1.getWeight());
		assertFalse(db1.isLocal());
		assertFalse(db1.isActive());
		assertFalse(db1.isDirty());
		
		DriverDatabase db2 = databases.get("db2");
		
		assertNotNull(db2);
		assertEquals("db2", db2.getId());
		assertEquals("jdbc:mock:db2", db2.getLocation());
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
