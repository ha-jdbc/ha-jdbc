package net.sf.ha.jdbc;

import java.util.HashMap;
import java.util.Map;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class DatabaseClusterDescriptor
{
	private String name;
	private String validateSQL;
	private Map databaseMap = new HashMap();
	
	/**
	 * @return Returns the databaseSet.
	 */
	public Map getDatabaseMap()
	{
		return this.databaseMap;
	}
	
	/**
	 * @param databaseSet The databaseSet to set.
	 */
	public void addDatabase(Object object)
	{
		Database database = (Database) object;
		this.databaseMap.put(database.getId(), database);
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
