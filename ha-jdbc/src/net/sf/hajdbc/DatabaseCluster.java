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
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
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
	 * @param manager
	 * @param descriptor
	 * @param databaseMap
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
	
	/**
	 * @return the descriptor for this database cluster
	 */
	public DatabaseClusterDescriptor getDescriptor()
	{
		return this.descriptor;
	}
	
	/**
	 * @param database
	 * @return true if the specified database is active, false otherwise
	 */
	public boolean isActive(Database database)
	{
		Connection connection = null;
		Statement statement = null;
		
		Object sqlObject = this.getSQLObject(database);
		
		try
		{
			connection = database.connect(sqlObject);
			
			statement = connection.createStatement();
			
			statement.execute(this.descriptor.getValidateSQL());
			
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
	 * Deactivates the specified database.
	 * @param database
	 * @return true if the database was successfully deactivated, false if it was already deactivated
	 */
	public boolean deactivate(Database database)
	{
		return this.manager.deactivate(this.descriptor.getName(), database);
	}
	
	public boolean activate(Database inactiveDatabase)
	{
		Connection activeConnection = null;
		Connection inactiveConnection = null;
		
		try
		{
			Database activeDatabase = this.getDescriptor().firstDatabase();
			
			inactiveConnection = inactiveDatabase.connect(this.getSQLObject(inactiveDatabase));
			inactiveConnection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
			inactiveConnection.setAutoCommit(false);
			
			activeConnection = activeDatabase.connect(this.getSQLObject(activeDatabase));
			activeConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			activeConnection.setAutoCommit(false);
			
			DatabaseMetaData metaData = inactiveConnection.getMetaData();
			ResultSet tableResultSet = metaData.getTables(null, null, "%", new String[] { "TABLE" });
			
			while (tableResultSet.next())
			{
				String table = tableResultSet.getString("TABLE_NAME");
				
				List primaryKeyList = new LinkedList();
				
				ResultSet primaryKeyResultSet = metaData.getPrimaryKeys(null, null, table);
				
				while (primaryKeyResultSet.next())
				{
					primaryKeyList.add(primaryKeyResultSet.getString("COLUMN_NAME"));
				}
				
				primaryKeyResultSet.close();
				
				Iterator primaryKeys = primaryKeyList.iterator();
				StringBuffer buffer = new StringBuffer("SELECT * FROM ").append(table).append(" ORDER BY ");
				
				while (primaryKeys.hasNext())
				{
					String primaryKey = (String) primaryKeys.next();
					
					buffer.append(primaryKey);
					
					if (primaryKeys.hasNext())
					{
						buffer.append(", ");
					}
				}
				
				String sql = buffer.toString();
				
				Statement inactiveStatement = inactiveConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
				Statement activeStatement = activeConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
				
				ResultSet inactiveResultSet = inactiveStatement.executeQuery(sql);
				ResultSet activeResultSet = activeStatement.executeQuery(sql);

				boolean hasActiveResults = activeResultSet.next();
				boolean hasInactiveResults = activeResultSet.next();

				int columns = activeResultSet.getMetaData().getColumnCount();
				
				while (hasActiveResults || hasInactiveResults)
				{
					int compare = 0;
					
					if (!hasActiveResults)
					{
						compare = 1;
					}
					else if (!hasInactiveResults)
					{
						compare = -1;
					}
					else
					{
						primaryKeys = primaryKeyList.iterator();
						
						while (primaryKeys.hasNext())
						{
							String primaryKey = (String) primaryKeys.next();
							
							Comparable activeObject = (Comparable) activeResultSet.getObject(primaryKey);
							Object inactiveObject = inactiveResultSet.getObject(primaryKey);
							
							compare = activeObject.compareTo(inactiveObject);
							
							if (compare != 0)
							{
								break;
							}
						}
					}
					
					if (compare == 0)
					{
						boolean updated = false;
						
						for (int i = 1; i <= columns; ++i)
						{
							Object activeObject = activeResultSet.getObject(i);
							Object inactiveObject = inactiveResultSet.getObject(i);
							
							if (activeResultSet.wasNull())
							{
								if (!inactiveResultSet.wasNull())
								{
									inactiveResultSet.updateNull(i);
									
									updated = true;
								}
							}
							else
							{
								if (inactiveResultSet.wasNull() || !activeObject.equals(inactiveObject))
								{
									inactiveResultSet.updateObject(1, activeObject);
									
									updated = true;
								}
							}
						}
						
						if (updated)
						{
							inactiveResultSet.updateRow();
						}
					}
					
					if (compare < 0)
					{
						inactiveResultSet.moveToInsertRow();

						for (int i = 1; i <= columns; ++i)
						{
							Object object = activeResultSet.getObject(i);
							
							if (activeResultSet.wasNull())
							{
								inactiveResultSet.updateNull(i);
							}
							else
							{
								inactiveResultSet.updateObject(i, object);
							}
						}
						
						inactiveResultSet.insertRow();
						inactiveResultSet.moveToCurrentRow();
					}
					
					if (compare > 0)
					{
						inactiveResultSet.deleteRow();
					}
					
					if (hasActiveResults)
					{
						hasActiveResults = activeResultSet.next();
					}
					
					if (hasInactiveResults)
					{
						hasInactiveResults = inactiveResultSet.next();
					}
				}
				
				inactiveConnection.commit();
			}
			
			activeConnection.rollback();
			activeConnection.close();
			
			tableResultSet.close();
			
			
			//return this.manager.activate(database);
			return true;
		}
		catch (SQLException e)
		{
			return false;
		}
		finally
		{
			if (activeConnection != null)
			{
				try
				{
					activeConnection.close();
				}
				catch (SQLException e)
				{
					// Ignore
				}
			}
			
			if (inactiveConnection != null)
			{
				try
				{
					inactiveConnection.close();
				}
				catch (SQLException e)
				{
					// Ignore
				}
			}
		}
	}
}
