package net.sf.ha.jdbc;

import java.util.Map;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class AbstractConnectionProxy extends SQLProxy
{
	private DatabaseCluster databaseCluster;
	
	/**
	 * Constructs a new AbstractConnectionProxy.
	 * @param databaseCluster
	 * @param connectionMap
	 */
	protected AbstractConnectionProxy(DatabaseCluster databaseCluster, Map connectionMap)
	{
		super(connectionMap);
		
		this.databaseCluster = databaseCluster;
	}
	
	/**
	 * @see net.sf.ha.jdbc.SQLProxy#getDatabaseCluster()
	 */
	protected DatabaseCluster getDatabaseCluster()
	{
		return this.databaseCluster;
	}
	
	/**
	 * @see net.sf.ha.jdbc.SQLProxy#deactivate(net.sf.ha.jdbc.Database)
	 */
	public void deactivate(Database database)
	{
		this.databaseCluster.deactivate(database);
		super.deactivate(database);
	}
}
