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
	
	/**
	 * @return
	 */
	public String getName()
	{
		return this.name;
	}
	
	/**
	 * @param name
	 */
	public void setName(String name)
	{
		this.name = name;
	}
	
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
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode()
	{
		return this.name.hashCode();
	}
	
	/**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object object)
	{
		if ((object == null) || !this.getClass().isInstance(object))
		{
			return false;
		}
		
		DataSourceDatabase database = (DataSourceDatabase) object;
		
		return this.name.equals(database.name);
	}

	/**
	 * @see net.sf.ha.jdbc.Database#connect(java.lang.Object)
	 */
	public Connection connect(Object object) throws SQLException
	{
		DataSource dataSource = (DataSource) object;
		
		return (this.user != null) ? dataSource.getConnection(this.user, this.password) : dataSource.getConnection();
	}
}
