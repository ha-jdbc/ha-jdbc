package net.sf.ha.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * Contains a map of <code>Database</code> -&gt; database connection factory (i.e. Driver, DataSource, ConnectionPoolDataSource, XADataSource)
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class DatabaseCluster extends AbstractProxy implements DatabaseEventListener
{
	private String name;
	private String validateSQL;
	
	protected DatabaseCluster(String name, Map databaseMap, String validateSQL)
	{
		super(databaseMap);
		
		this.name = name;
		this.validateSQL = validateSQL;
	}
	
	public String getName()
	{
		return this.name;
	}

	public String getValidateSQL()
	{
		return this.validateSQL;
	}
	
	protected DatabaseCluster getDatabaseCluster()
	{
		return this;
	}
	
	public boolean isActive(Database database)
	{
		Connection connection = null;
		PreparedStatement statement = null;
		
		Object object = this.objectMap.get(database);
		
		try
		{
			connection = database.connect(object);
			
			statement = connection.prepareStatement(this.validateSQL);
			
			statement.executeQuery();
			
			return true;
		}
		catch (SQLException e)
		{
			return false;
		}
		finally
		{
			if (statement != null)
			{
				try
				{
					statement.close();
				}
				catch (SQLException e)
				{
					// Ignore
				}
			}

			if (connection != null)
			{
				try
				{
					connection.close();
				}
				catch (SQLException e)
				{
					// Ignore
				}
			}
		}
	}
	
	/**
	 * @see net.sf.ha.jdbc.DatabaseActivationEventListener#deactivated(net.sf.ha.jdbc.DatabaseActivationEvent)
	 */
	public void deactivated(DatabaseEvent event)
	{
	}
}
