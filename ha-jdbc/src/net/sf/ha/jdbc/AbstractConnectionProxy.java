package net.sf.ha.jdbc;

import java.util.Map;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class AbstractConnectionProxy extends AbstractProxy
{
	private DatabaseManager databaseManager;
	
	protected AbstractConnectionProxy(DatabaseManager databaseManager, Map connectionMap)
	{
		super(connectionMap);
		
		this.databaseManager = databaseManager;
	}
	
	/**
	 * @see net.sf.hajdbc.AbstractProxy#getDatabaseManager()
	 */
	protected DatabaseManager getDatabaseManager()
	{
		return this.databaseManager;
	}
	
	public void deactivate(Database database)
	{
		this.databaseManager.deactivate(database);
		super.deactivate(database);
	}
}
