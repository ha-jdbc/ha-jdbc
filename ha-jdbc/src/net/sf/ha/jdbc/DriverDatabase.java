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

	/**
	 * @return
	 */
	public String getUser()
	{
		return this.user;
	}
	
	/**
	 * @param user
	 */
	public void setUser(String user)
	{
		this.user = user;
	}
	
	/**
	 * @return
	 */
	public String getPassword()
	{
		return this.password;
	}
	
	/**
	 * @param password
	 */
	public void setPassword(String password)
	{
		this.password = password;
	}
	
	/**
	 * @return
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
	 * @return
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
	 * @return
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
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode()
	{
		return this.url.hashCode();
	}
	
	/**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
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
	/**
	 * @see net.sf.ha.jdbc.Database#connect(java.lang.Object)
	 */
	public Connection connect(Object object) throws SQLException
	{
		Driver driver = (Driver) object;
		
		return driver.connect(this.url, this.getProperties());
	}
}
