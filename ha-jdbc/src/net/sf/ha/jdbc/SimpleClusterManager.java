package net.sf.ha.jdbc;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class SimpleClusterManager extends DatabaseClusterManager
{
	public static DatabaseClusterManager instance = new SimpleClusterManager();
	
	private SimpleClusterManager()
	{
		super();
	}
	
	public static DatabaseClusterManager getInstance()
	{
		return instance;
	}
}
