package net.sf.ha.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class DriverDatabase implements Database
{
	private static final String USER = "user";
	private static final String PASSWORD = "password";
	
	private String url;
	private String driver;
	private String user;
	private String password;

	public String getUser()
	{
		return this.user;
	}
	
	public void setUser(String user)
	{
		this.user = user;
	}
	
	public String getPassword()
	{
		return this.password;
	}
	
	public void setPassword(String password)
	{
		this.password = password;
	}
	
	public String getUrl()
	{
		return this.url;
	}
	
	public void setUrl(String url)
	{
		this.url = url;
	}
	
	public String getDriver()
	{
		return this.driver;
	}
	
	public void setDriver(String driver)
	{
		this.driver = driver;
	}
	
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
	
	public void setProperties(Properties properties)
	{
		this.user = properties.getProperty(USER);
		this.password = properties.getProperty(PASSWORD);
	}
	
	public int hashCode()
	{
		return this.url.hashCode();
	}
	
	public boolean equals(Object object)
	{
		if ((object == null) || !DriverDatabase.class.isInstance(object))
		{
			return false;
		}
		
		DriverDatabase database = (DriverDatabase) object;
		
		return this.url.equals(database.url);
	}

	/**
	 * @see net.sf.ha.jdbc.ConnectionInfo#connect(java.lang.Object)
	 */
	public Connection connect(Object object) throws SQLException
	{
		Driver driver = (Driver) object;
		
		return driver.connect(this.url, this.getProperties());
	}
}
