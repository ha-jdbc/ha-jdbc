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
package net.sf.hajdbc.sql.xa;

import java.sql.Connection;
import java.sql.SQLException;

import javax.naming.spi.ObjectFactory;
import javax.sql.XAConnection;
import javax.sql.XADataSource;

import net.sf.hajdbc.sql.TestCommonDataSourceDatabase;

import org.easymock.EasyMock;

/**
 * @author Paul Ferraro
 *
 */
public class TestXADataSourceDatabase extends TestCommonDataSourceDatabase<XADataSourceDatabase, XADataSource>
{
	public TestXADataSourceDatabase()
	{
		super(XADataSource.class);
	}

	/**
	 * @see net.sf.hajdbc.sql.TestDatabase#createDatabase(java.lang.String)
	 */
	@Override
	protected XADataSourceDatabase createDatabase(String id)
	{
		XADataSourceDatabase database = new XADataSourceDatabase();
		
		database.setId(id);
		
		return database;
	}

	/**
	 * @see net.sf.hajdbc.sql.TestCommonDataSourceDatabase#mockDataSourceClass()
	 */
	@Override
	protected Class<? extends XADataSource> mockDataSourceClass()
	{
		return MockXADataSource.class;
	}

	/**
	 * @see net.sf.hajdbc.sql.TestCommonDataSourceDatabase#objectFactoryClass()
	 */
	@Override
	protected Class<? extends ObjectFactory> objectFactoryClass()
	{
		return MockXADataSourceFactory.class;
	}

	@Override
	public void testConnect() throws SQLException
	{
		XADataSource dataSource = EasyMock.createStrictMock(XADataSource.class);
		XAConnection xaConnection = EasyMock.createStrictMock(XAConnection.class);
		
		Connection connection = EasyMock.createMock(Connection.class);
		
		EasyMock.expect(dataSource.getXAConnection()).andReturn(xaConnection);
		EasyMock.expect(xaConnection.getConnection()).andReturn(connection);
		
		EasyMock.replay(dataSource, xaConnection);
		
		Connection result = this.connect(dataSource);
		
		EasyMock.verify(dataSource, xaConnection);
		
		assert result == connection : result.getClass().getName();

		EasyMock.reset(dataSource, xaConnection);
		
		this.database.setUser("user");
		this.database.setPassword("password");

		EasyMock.expect(dataSource.getXAConnection("user", "password")).andReturn(xaConnection);
		EasyMock.expect(xaConnection.getConnection()).andReturn(connection);
		
		EasyMock.replay(dataSource, xaConnection);
		
		result = this.connect(dataSource);
		
		EasyMock.verify(dataSource, xaConnection);
		
		assert result == connection : result.getClass().getName();
	}
}
