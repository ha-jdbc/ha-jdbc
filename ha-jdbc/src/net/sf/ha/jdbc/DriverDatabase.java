package net.sf.ha.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class DriverDatabase implements Database
{
	private String url;
	private Properties properties;
	
	public DriverDatabase(String url)
	{
		this.url = url;
	}
	
	public String getUrl()
	{
		return this.url;
	}
	
	public Properties getProperties()
	{
		return this.properties;
	}
	
	public void setProperties(Properties properties)
	{
		this.properties = properties;
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
		
		return driver.connect(this.url, this.properties);
	}
}
