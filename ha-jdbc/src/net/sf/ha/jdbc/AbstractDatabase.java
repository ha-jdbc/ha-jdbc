package net.sf.ha.jdbc;


/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public abstract class AbstractDatabase implements Database
{
	protected String user;
	protected String password;
	
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
		return this.getId().hashCode();
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
		
		return this.getId().equals(database.name);
	}
}
