/*
 * Copyright (c) 2004, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class UpdateDatabaseActivationStrategy implements DatabaseActivationStrategy
{
	private static Log log = LogFactory.getLog(UpdateDatabaseActivationStrategy.class);
	
	/**
	 * @see net.sf.hajdbc.DatabaseActivationStrategy#activate(net.sf.hajdbc.DatabaseCluster, net.sf.hajdbc.Database)
	 */
	public void activate(DatabaseCluster databaseCluster, Database database) throws SQLException
	{
		Connection activeConnection = null;
		Connection inactiveConnection = null;
		
		List foreignKeyList = new LinkedList();
		
		try
		{
			Database activeDatabase = databaseCluster.getDescriptor().firstDatabase();
			
			inactiveConnection = database.connect(databaseCluster.getSQLObject(database));
			inactiveConnection.setAutoCommit(true);
			
			DatabaseMetaData databaseMetaData = inactiveConnection.getMetaData();
			List tableList = new LinkedList();
			ResultSet resultSet = databaseMetaData.getTables(null, null, "%", new String[] { "TABLE" });
			
			while (resultSet.next())
			{
				String table = resultSet.getString("TABLE_NAME");
				tableList.add(table);
			}
			
			resultSet.close();

			Iterator tables = tableList.iterator();
			
			while (tables.hasNext())
			{
				String table = (String) tables.next();
				
				resultSet = databaseMetaData.getImportedKeys(null, null, table);
				
				while (resultSet.next())
				{
					String name = resultSet.getString("FK_NAME");
					String column = resultSet.getString("FKCOLUMN_NAME");
					String foreignTable = resultSet.getString("PKTABLE_NAME");
					String foreignColumn = resultSet.getString("PKCOLUMN_NAME");
					
					ForeignKey foreignKey = new ForeignKey(name, table, column, foreignTable, foreignColumn);
					String sql = foreignKey.dropSQL();
					
					log.info("Dropping foreign key: " + sql);

					Statement statement = inactiveConnection.createStatement();
					statement.execute(sql);
					statement.close();
					
					foreignKeyList.add(foreignKey);
				}
				
				resultSet.close();
			}
			
			activeConnection = activeDatabase.connect(databaseCluster.getSQLObject(activeDatabase));
			activeConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			activeConnection.setAutoCommit(false);
			
			tables = tableList.iterator();
			
			while (tables.hasNext())
			{
				String table = (String) tables.next();
				
				List primaryKeyList = new LinkedList();
				
				resultSet = databaseMetaData.getPrimaryKeys(null, null, table);
				
				while (resultSet.next())
				{
					primaryKeyList.add(resultSet.getString("COLUMN_NAME"));
				}
				
				resultSet.close();
				
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
				Statement activeStatement = activeConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				
				log.info("Updating: " + sql);
				ResultSet inactiveResultSet = inactiveStatement.executeQuery(sql);
				ResultSet activeResultSet = activeStatement.executeQuery(sql);
				
				boolean hasActiveResults = activeResultSet.next();
				boolean hasInactiveResults = inactiveResultSet.next();

				ResultSetMetaData resultSetMetaData = activeResultSet.getMetaData();
				int columns = resultSetMetaData.getColumnCount();
				
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
							String column = resultSetMetaData.getColumnName(i);
							Object activeObject = activeResultSet.getObject(i);
							Object inactiveObject = inactiveResultSet.getObject(column);
							
							if (activeResultSet.wasNull())
							{
								if (!inactiveResultSet.wasNull())
								{
									inactiveResultSet.updateNull(column);
									
									updated = true;
								}
							}
							else
							{
								if (inactiveResultSet.wasNull() || !activeObject.equals(inactiveObject))
								{
									inactiveResultSet.updateObject(column, activeObject);
									
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
							String column = resultSetMetaData.getColumnName(i);
							Object object = activeResultSet.getObject(i);
							
							if (activeResultSet.wasNull())
							{
								inactiveResultSet.updateNull(column);
							}
							else
							{
								inactiveResultSet.updateObject(column, object);
							}
						}
						
						inactiveResultSet.insertRow();
						inactiveResultSet.moveToCurrentRow();
					}
					
					if (compare > 0)
					{
						inactiveResultSet.deleteRow();
					}
					
					if (hasActiveResults && (compare <= 0))
					{
						hasActiveResults = activeResultSet.next();
					}
					
					if (hasInactiveResults &&  (compare >= 0))
					{
						hasInactiveResults = inactiveResultSet.next();
					}
				}
				
				inactiveStatement.close();
				activeStatement.close();
			}

			log.info("Database synchronization completed successfully");
		}
		finally
		{
			if ((activeConnection != null) && !activeConnection.isClosed())
			{
				try
				{
					activeConnection.close();
				}
				catch (SQLException sqle)
				{
					log.warn("Failed to close connection of active database", sqle);
				}
			}
			
			if ((inactiveConnection != null) && !inactiveConnection.isClosed())
			{
				try
				{
					Iterator foreignKeys = foreignKeyList.iterator();
					
					while (foreignKeys.hasNext())
					{
						ForeignKey foreignKey = (ForeignKey) foreignKeys.next();
						String sql = foreignKey.createSQL();
						
						log.info("Recreating foreign key: " + sql);
						
						Statement statement = inactiveConnection.createStatement();
						statement.execute(sql);
						statement.close();
					}
					
					inactiveConnection.close();
				}
				catch (SQLException e)
				{
					log.warn("Failed to close connection of inactive database", e);
				}
			}
		}
	}
}
