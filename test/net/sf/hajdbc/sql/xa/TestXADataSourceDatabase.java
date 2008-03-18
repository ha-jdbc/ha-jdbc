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
	private XAConnection connection = EasyMock.createStrictMock(XAConnection.class);

	public TestXADataSourceDatabase()
	{
		super(EasyMock.createStrictMock(XADataSource.class));
	}
	
	/**
	 * @see net.sf.hajdbc.sql.TestCommonDataSourceDatabase#objects()
	 */
	@Override
	protected Object[] objects()
	{
		return new Object[] { this.dataSource, this.connection };
	}

	/**
	 * @see net.sf.hajdbc.sql.TestCommonDataSourceDatabase#createDatabase()
	 */
	@Override
	protected XADataSourceDatabase createDatabase()
	{
		return new XADataSourceDatabase();
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

	/**
	 * @see net.sf.hajdbc.Database#connect(java.lang.Object)
	 */
	@Override
	public Connection connect(XADataSource dataSource) throws SQLException
	{
		XADataSourceDatabase database = this.createDatabase("1");
		
		Connection connection = EasyMock.createMock(Connection.class);
		
		EasyMock.expect(this.dataSource.getXAConnection()).andReturn(this.connection);
		EasyMock.expect(this.connection.getConnection()).andReturn(connection);
		
		this.replay();
		
		Connection result = database.connect(dataSource);
		
		this.verify();
		
		assert result == connection : result.getClass().getName();
		
		database.setUser("user");
		database.setPassword("password");

		EasyMock.expect(this.dataSource.getXAConnection("user", "password")).andReturn(this.connection);
		EasyMock.expect(this.connection.getConnection()).andReturn(connection);
		
		this.replay();
		
		result = database.connect(dataSource);
		
		this.verify();
		
		assert result == connection : result.getClass().getName();
		
		return result;
	}
}
