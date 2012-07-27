/*
 * HA-JDBC: High-Availablity JDBC
 * Copyright 2011 Paul Ferraro
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
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseClusterConfigurationFactory;
import net.sf.hajdbc.DatabaseClusterFactory;
import net.sf.hajdbc.MockDriver;
import net.sf.hajdbc.balancer.Balancer;
import net.sf.hajdbc.durability.Durability;
import net.sf.hajdbc.lock.LockManager;
import net.sf.hajdbc.tx.TransactionIdentifierFactory;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

/**
 * @author Paul Ferraro
 */
public class DriverTest
{
	@SuppressWarnings("unchecked")
	private DatabaseClusterConfigurationFactory<java.sql.Driver, DriverDatabase> configurationFactory = mock(DatabaseClusterConfigurationFactory.class);
	@SuppressWarnings("unchecked")
	private DatabaseClusterFactory<java.sql.Driver, DriverDatabase> factory = mock(DatabaseClusterFactory.class);
	private Connection connection = mock(Connection.class);
	
	private java.sql.Driver mockDriver = new MockDriver(this.connection);

	private Driver driver;
	
	@Before
	public void before() throws SQLException
	{
		DriverManager.setLogWriter(new java.io.PrintWriter(System.out));
		DriverManager.registerDriver(this.mockDriver);
		
		this.driver = (Driver) DriverManager.getDriver("jdbc:ha-jdbc:cluster");
		this.driver.setFactory(this.factory);
		this.driver.getConfigurationFactories().put("cluster", this.configurationFactory);
	}
	
	@After
	public void after() throws SQLException
	{
		DriverManager.deregisterDriver(this.mockDriver);
	}
	
	@Test
	public void acceptsURL() throws SQLException
	{
		Assert.assertTrue(this.driver.acceptsURL("jdbc:ha-jdbc:cluster"));
		Assert.assertTrue(this.driver.acceptsURL("jdbc:ha-jdbc://cluster"));
		Assert.assertTrue(this.driver.acceptsURL("jdbc:ha-jdbc://cluster/database"));
		Assert.assertFalse(this.driver.acceptsURL("jdbc:postgresql:database"));
		Assert.assertFalse(this.driver.acceptsURL("jdbc:postgresql://server"));
		Assert.assertFalse(this.driver.acceptsURL("jdbc:postgresql://server/database"));
	}
	
	@Test
	public void connect() throws Exception
	{
		this.connect("jdbc:ha-jdbc:cluster1", "cluster1");
		this.connect("jdbc:ha-jdbc://cluster2", "cluster2");
		this.connect("jdbc:ha-jdbc://cluster3/dummy", "cluster3");
	}
	
	@SuppressWarnings("unchecked")
	private void connect(String url, String id) throws Exception
	{
		DatabaseCluster<java.sql.Driver, DriverDatabase> cluster = mock(DatabaseCluster.class);
		Balancer<java.sql.Driver, DriverDatabase> balancer = mock(Balancer.class);
		LockManager lockManager = mock(LockManager.class);
		
		DriverDatabase database = new DriverDatabase();
		database.setId("db1");
		database.setName("jdbc:mock:test");
		
		Driver driver = new Driver();

		driver.setFactory(this.factory);
		driver.getConfigurationFactories().put(id, this.configurationFactory);

		when(this.factory.createDatabaseCluster(eq(id), same(this.configurationFactory))).thenReturn(cluster);
		cluster.start();
		
		when(cluster.isActive()).thenReturn(true);
		when(cluster.getBalancer()).thenReturn(balancer);
		when(balancer.iterator()).thenReturn(Collections.singleton(database).iterator());
		when(cluster.getBalancer()).thenReturn(balancer);
		when(balancer.contains(database)).thenReturn(true);
		when(balancer.isEmpty()).thenReturn(false);
		when(balancer.size()).thenReturn(1);
		when(balancer.iterator()).thenReturn(Collections.singleton(database).iterator());
		when(balancer.next()).thenReturn(database);
		when(cluster.getExecutor()).thenReturn(Executors.newCachedThreadPool());
		when(cluster.getLockManager()).thenReturn(lockManager);
		when(lockManager.readLock(null)).thenReturn(mock(Lock.class));
		when(cluster.getDurability()).thenReturn(mock(Durability.class));
		when(cluster.getTransactionIdentifierFactory()).thenReturn(mock(TransactionIdentifierFactory.class));
		
		Connection result = driver.connect(url, null);
		
//		Assert.assertSame(this.connection, result);
	}
}
