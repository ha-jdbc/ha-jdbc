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
public class DriverDatabase extends AbstractDatabase
{
	private static final String USER = "user";
	private static final String PASSWORD = "password";
	
	private String url;
	private String driver;
	
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
	 * @see net.sf.ha.jdbc.Database#getId()
	 */
	public String getId()
	{
		return this.url;
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
