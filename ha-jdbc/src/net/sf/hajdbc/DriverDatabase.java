/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2004 Paul Ferraro
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
package net.sf.hajdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class DriverDatabase extends AbstractDatabase
{
	private static final String USER = "user";
	private static final String PASSWORD = "password";
	
	private String url;
	private String driver;
	
	/**
	 * @return the database url
	 */
	public String getUrl()
	{
		return this.url;
	}
	
	/**
	 * @param url
	 */
	public void setUrl(String url)
	{
		this.url = url;
	}
	
	/**
	 * @return the database driver class name 
	 */
	public String getDriver()
	{
		return this.driver;
	}
	
	/**
	 * @param driver
	 */
	public void setDriver(String driver)
	{
		this.driver = driver;
	}
	
	/**
	 * @return the user and password as a set of properties
	 */
	public Properties getProperties()
	{
		Properties properties = new Properties();
		
		if (this.user != null)
		{
			properties.setProperty(USER, this.user);
		}
		
		if (this.password != null)
		{
			properties.setProperty(PASSWORD, this.password);
		}
		
		return properties;
	}
	
	/**
	 * @param properties
	 */
	public void setProperties(Properties properties)
	{
		this.user = properties.getProperty(USER);
		this.password = properties.getProperty(PASSWORD);
	}
	
	/**
	 * @see net.sf.hajdbc.Database#connect(java.lang.Object)
	 */
	public Connection connect(Object connectionFactory) throws java.sql.SQLException
	{
		Driver driver = (Driver) connectionFactory;
		
		return driver.connect(this.url, this.getProperties());
	}
	
	public Object getConnectionFactory() throws java.sql.SQLException
	{
		if (this.driver != null)
		{
			try
			{
				Class driverClass = Class.forName(this.driver);
				
				if (!Driver.class.isAssignableFrom(driverClass))
				{
					throw new SQLException(Messages.getMessage(Messages.NOT_INSTANCE_OF, new Object[] { this.driver, Driver.class.getName() }));
				}
			}
			catch (ClassNotFoundException e)
			{
				throw new SQLException(Messages.getMessage(Messages.CLASS_NOT_FOUND, this.driver), e);
			}
		}
		
		try
		{
			return DriverManager.getDriver(this.url);
		}
		catch (java.sql.SQLException e)
		{
			throw new SQLException(Messages.getMessage(Messages.JDBC_URL_REJECTED, new Object[] { this.url }), e);
		}
	}
}
