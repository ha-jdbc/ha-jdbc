package net.sf.ha.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class DataSourceDatabase implements Database
{
	protected String name;
	protected String user;
	protected String password;
	
	public DataSourceDatabase(String name)
	{
		this.name = name;
	}
	
	public String getName()
	{
		return this.name;
	}
	
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
	
	public int hashCode()
	{
		return this.name.hashCode();
	}
	
	public boolean equals(Object object)
	{
		if ((object == null) || !DataSourceDatabase.class.isInstance(object))
		{
			return false;
		}
		
		DataSourceDatabase database = (DataSourceDatabase) object;
		
		return this.name.equals(database.name);
	}

	/**
	 * @see net.sf.ha.jdbc.ConnectionInfo#connect(java.lang.Object)
	 */
	public Connection connect(Object object) throws SQLException
	{
		DataSource dataSource = (DataSource) object;
		
		return (this.user != null) ? dataSource.getConnection(this.user, this.password) : dataSource.getConnection();
	}
}
