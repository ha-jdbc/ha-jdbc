package net.sf.ha.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class DataSourceDatabase extends AbstractDatabase
{
	protected String name;
	
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
	 * @see net.sf.ha.jdbc.Database#getId()
	 */
	public String getId()
	{
		return this.name;
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
