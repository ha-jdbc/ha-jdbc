package net.sf.ha.jdbc;

import java.util.Map;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class AbstractConnectionProxy extends AbstractProxy
{
	private DatabaseCluster databaseCluster;
	
	protected AbstractConnectionProxy(DatabaseCluster databaseCluster, Map connectionMap)
	{
		super(connectionMap);
		
		this.databaseCluster = databaseCluster;
	}
	
	/**
	 * @see net.sf.hajdbc.AbstractProxy#getDatabaseCluster()
	 */
	protected DatabaseCluster getDatabaseCluster()
	{
		return this.databaseCluster;
	}
	
	public void deactivate(Database database)
	{
		this.databaseCluster.deactivate(database);
		super.deactivate(database);
	}
}
