package net.sf.ha.jdbc;

import java.util.Set;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class DatabaseClusterDescriptor
{
	private String name;
	private String validateSQL;
	private Set databaseSet;
	
	/**
	 * @return Returns the databaseSet.
	 */
	public Set getDatabaseSet()
	{
		return this.databaseSet;
	}
	
	/**
	 * @param databaseSet The databaseSet to set.
	 */
	public void setDatabaseSet(Set databaseSet)
	{
		this.databaseSet = databaseSet;
	}
	
	/**
	 * @return Returns the name.
	 */
	public String getName()
	{
		return this.name;
	}
	
	/**
	 * @param name The name to set.
	 */
	public void setName(String name)
	{
		this.name = name;
	}
	
	/**
	 * @return Returns the validateSQL.
	 */
	public String getValidateSQL()
	{
		return this.validateSQL;
	}
	
	/**
	 * @param validateSQL The validateSQL to set.
	 */
	public void setValidateSQL(String validateSQL)
	{
		this.validateSQL = validateSQL;
	}
}
