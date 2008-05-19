/**
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
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
package net.sf.hajdbc.sql;

import java.sql.Connection;
import java.sql.SQLException;

import javax.naming.spi.ObjectFactory;
import javax.sql.DataSource;

import org.easymock.EasyMock;
import org.testng.annotations.Test;

/**
 * Unit test for {@link DataSourceDatabase}.
 * @author  Paul Ferraro
 * @since   1.1
 */
@Test
@SuppressWarnings("nls")
public class TestDataSourceDatabase extends TestCommonDataSourceDatabase<DataSourceDatabase, DataSource>
{
	public TestDataSourceDatabase()
	{
		super(new DataSourceDatabase(), DataSource.class);
	}

	/**
	 * @see net.sf.hajdbc.sql.TestCommonDataSourceDatabase#mockDataSourceClass()
	 */
	@Override
	protected Class<? extends DataSource> mockDataSourceClass()
	{
		return MockDataSource.class;
	}

	/**
	 * @see net.sf.hajdbc.sql.TestCommonDataSourceDatabase#objectFactoryClass()
	 */
	@Override
	protected Class<? extends ObjectFactory> objectFactoryClass()
	{
		return MockDataSourceFactory.class;
	}
	
	@Override
	public void testConnect() throws SQLException
	{
		DataSource dataSource = EasyMock.createStrictMock(DataSource.class);
		Connection connection = EasyMock.createMock(Connection.class);
		
		EasyMock.expect(dataSource.getConnection()).andReturn(connection);
		
		EasyMock.replay(dataSource);
		
		Connection result = this.connect(dataSource);
		
		EasyMock.verify(dataSource);
		
		assert result == connection : result.getClass().getName();
		
		EasyMock.reset(dataSource);
		
		this.database.setUser("user");
		this.database.setPassword("password");

		EasyMock.expect(dataSource.getConnection("user", "password")).andReturn(connection);
		
		EasyMock.replay(dataSource);
		
		result = this.connect(dataSource);
		
		EasyMock.verify(dataSource);
		
		assert result == connection : result.getClass().getName();
	}
}
