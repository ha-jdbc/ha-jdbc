/**
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
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

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.sql.DataSource;

import net.sf.hajdbc.ActiveDatabaseMBean;
import net.sf.hajdbc.InactiveDatabaseMBean;

import org.easymock.EasyMock;
import org.testng.annotations.Configuration;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test for {@link DataSourceDatabase}.
 * @author  Paul Ferraro
 * @since   1.1
 */
public class TestDataSourceDatabase extends AbstractTestDatabase<DataSourceDatabase, DataSource> implements InactiveDataSourceDatabaseMBean
{
	private DataSource dataSource = this.control.createMock(DataSource.class);

	@Override
	@Configuration(beforeTestMethod = true)
	protected void setup()
	{
		super.setup();
		
		try
		{
			Reference reference = new Reference(this.dataSource.getClass().getName(), DataSourceFactory.class.getName(), null);
			
			Context context = new InitialContext(this.database.getProperties());
			
			context.rebind("test", reference);
		}
		catch (NamingException e)
		{
			assert false;
		}
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractTestDatabase#createDatabase(java.lang.String)
	 */
	@Override
	protected DataSourceDatabase createDatabase(String id)
	{
		DataSourceDatabase database = new DataSourceDatabase();
		
		database.setId(id);
		database.setName("test");
		database.getProperties().setProperty(Context.INITIAL_CONTEXT_FACTORY, MockInitialContextFactory.class.getName());
		
		return database;
	}

	@DataProvider(name = "datasource")
	protected Object[][] dataSourceProvider()
	{
		return new Object[][] { new Object[] { this.dataSource } };
	}
	
	/**
	 * @see net.sf.hajdbc.Database#connect(T)
	 */
	@Test(dataProvider = "datasource")
	public Connection connect(DataSource connectionFactory) throws SQLException
	{
		Connection connection = EasyMock.createMock(Connection.class);
		
		EasyMock.expect(this.dataSource.getConnection()).andReturn(connection);
		
		this.control.replay();
		
		Connection c = this.database.connect(connectionFactory);
		
		this.control.verify();
		
		assert c == connection : c.getClass().getName();
		
		return null;
	}

	/**
	 * @see net.sf.hajdbc.Database#createConnectionFactory()
	 */
	@Test
	public DataSource createConnectionFactory()
	{
		DataSource dataSource = this.database.createConnectionFactory();
		
		String dataSourceClass = dataSource.getClass().getName();
		
		assert dataSourceClass.equals("net.sf.hajdbc.sql.MockDataSource") : dataSourceClass;
		
		return dataSource;
	}

	/**
	 * @see net.sf.hajdbc.Database#getActiveMBeanClass()
	 */
	@Test
	public Class<? extends ActiveDatabaseMBean> getActiveMBeanClass()
	{
		Class<? extends ActiveDatabaseMBean> mbeanClass = this.database.getActiveMBeanClass();
		
		assert mbeanClass.equals(ActiveDataSourceDatabaseMBean.class) : mbeanClass.getName();
		
		return mbeanClass;
	}

	/**
	 * @see net.sf.hajdbc.Database#getInactiveMBeanClass()
	 */
	@Test
	public Class<? extends InactiveDatabaseMBean> getInactiveMBeanClass()
	{
		Class<? extends InactiveDatabaseMBean> mbeanClass = this.database.getInactiveMBeanClass();
		
		assert mbeanClass.equals(InactiveDataSourceDatabaseMBean.class) : mbeanClass.getName();
		
		return mbeanClass;
	}

	/**
	 * @see net.sf.hajdbc.sql.ActiveDataSourceDatabaseMBean#getName()
	 */
	@Test
	public String getName()
	{
		String name = this.database.getName();
		
		assert name.equals("test") : name;
		
		return name;
	}
}
