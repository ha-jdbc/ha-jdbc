package net.sf.ha.jdbc;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class SimpleDatabaseClusterManager extends DatabaseClusterManager
{
	public static DatabaseClusterManager instance = new SimpleDatabaseClusterManager();
	
	private SimpleDatabaseClusterManager()
	{
		super();
	}
	
	public static DatabaseClusterManager getInstance()
	{
		return instance;
	}
}
