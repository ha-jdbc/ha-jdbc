/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2004 Paul Ferraro
 * 
 * This library is free software; you can redistribute it and/or modify it 
 * under the terms of the GNU Lesser General Public License as published by the 
 * Free Software Foundation; either version 2.1 of the License, or (at your 
 * option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License 
 * for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, 
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 * 
 * Contact: ferraro@users.sourceforge.net
 */
package net.sf.hajdbc;

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
public class DatabaseCluster extends SQLProxy
{
	private DatabaseClusterDescriptor descriptor;
	private DatabaseClusterManager manager;
	
	/**
	 * Constructs a new DatabaseCluster.
	 * @param name
	 * @param databaseMap
	 * @param validateSQL
	 */
	protected DatabaseCluster(DatabaseClusterManager manager, DatabaseClusterDescriptor descriptor, Map databaseMap)
	{
		super(databaseMap);
		
		this.descriptor = descriptor;
		this.manager = manager;
	}
	
	/**
	 * @see net.sf.hajdbc.SQLProxy#getDatabaseCluster()
	 */
	protected DatabaseCluster getDatabaseCluster()
	{
		return this;
	}
	
	public DatabaseClusterDescriptor getDescriptor()
	{
		return this.descriptor;
	}
	
	/**
	 * @param database
	 * @return
	 */
	public boolean isActive(Database database)
	{
		Connection connection = null;
		PreparedStatement statement = null;
		
		Object object = this.getObject(database);
		
		try
		{
			connection = database.connect(object);
			
			statement = connection.prepareStatement(this.descriptor.getValidateSQL());
			
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
	
	public void deactivate(Database database)
	{
		this.manager.deactivate(this.descriptor.getName(), database);
	}
}
