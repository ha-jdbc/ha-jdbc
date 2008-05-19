/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2008 Paul Ferraro
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
package net.sf.hajdbc.sql.pool;

import java.sql.Connection;
import java.sql.SQLException;

import javax.naming.spi.ObjectFactory;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

import net.sf.hajdbc.sql.TestCommonDataSourceDatabase;

import org.easymock.EasyMock;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
@Test
@SuppressWarnings("nls")
public class TestConnectionPoolDataSourceDatabase extends TestCommonDataSourceDatabase<ConnectionPoolDataSourceDatabase, ConnectionPoolDataSource>
{
	public TestConnectionPoolDataSourceDatabase()
	{
		super(new ConnectionPoolDataSourceDatabase(), ConnectionPoolDataSource.class);
	}

	/**
	 * @see net.sf.hajdbc.sql.TestCommonDataSourceDatabase#mockDataSourceClass()
	 */
	@Override
	protected Class<? extends ConnectionPoolDataSource> mockDataSourceClass()
	{
		return MockConnectionPoolDataSource.class;
	}

	/**
	 * @see net.sf.hajdbc.sql.TestCommonDataSourceDatabase#objectFactoryClass()
	 */
	@Override
	protected Class<? extends ObjectFactory> objectFactoryClass()
	{
		return MockConnectionPoolDataSourceFactory.class;
	}

	/**
	 * @see net.sf.hajdbc.sql.TestDatabase#testConnect()
	 */
	@Override
	public void testConnect() throws SQLException
	{
		ConnectionPoolDataSource dataSource = EasyMock.createStrictMock(ConnectionPoolDataSource.class);
		PooledConnection pooledConnection = EasyMock.createStrictMock(PooledConnection.class);
		Connection connection = EasyMock.createMock(Connection.class);
		
		EasyMock.expect(dataSource.getPooledConnection()).andReturn(pooledConnection);
		EasyMock.expect(pooledConnection.getConnection()).andReturn(connection);
		
		EasyMock.replay(dataSource, pooledConnection);
		
		Connection result = this.connect(dataSource);
		
		EasyMock.verify(dataSource, pooledConnection);
		
		assert result == connection : result.getClass().getName();

		EasyMock.reset(dataSource, pooledConnection);
		
		this.database.setUser("user");
		this.database.setPassword("password");

		EasyMock.expect(dataSource.getPooledConnection("user", "password")).andReturn(pooledConnection);
		EasyMock.expect(pooledConnection.getConnection()).andReturn(connection);
		
		EasyMock.replay(dataSource, pooledConnection);
		
		result = this.database.connect(dataSource);
		
		EasyMock.verify(dataSource, pooledConnection);
		
		assert result == connection : result.getClass().getName();
	}
}
