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
package net.sf.hajdbc.synch;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseSynchronizationStrategy;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseClusterDescriptor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class DiffDatabaseSynchronizationStrategy implements DatabaseSynchronizationStrategy
{
	private static Log log = LogFactory.getLog(DiffDatabaseSynchronizationStrategy.class);
	
	/**
	 * @see net.sf.hajdbc.DatabaseActivationStrategy#activate(net.sf.hajdbc.DatabaseCluster, net.sf.hajdbc.Database)
	 */
	public void synchronize(DatabaseCluster databaseCluster, Database database) throws SQLException
	{
		DatabaseClusterDescriptor descriptor = databaseCluster.getDescriptor();
		Connection activeConnection = null;
		Connection inactiveConnection = null;
		
		List tableList = new LinkedList();
		List foreignKeyList = new LinkedList();
		List primaryKeyList = new ArrayList();
		Set primaryKeyColumnSet = new LinkedHashSet();
		
		try
		{
			Database activeDatabase = databaseCluster.nextDatabase();
			
			inactiveConnection = database.connect(databaseCluster.getSQLObject(database));
			inactiveConnection.setAutoCommit(false);
			
			DatabaseMetaData databaseMetaData = inactiveConnection.getMetaData();
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
					String sql = foreignKey.formatSQL(descriptor.getDropForeignKeySQL());
					
					log.info("Dropping foreign key: " + sql);

					Statement statement = inactiveConnection.createStatement();
					statement.execute(sql);
					statement.close();
					
					foreignKeyList.add(foreignKey);
				}
				
				resultSet.close();
			}

			inactiveConnection.commit();
			
			activeConnection = activeDatabase.connect(databaseCluster.getSQLObject(activeDatabase));
			activeConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			activeConnection.setAutoCommit(false);
			
			tables = tableList.iterator();
			
			while (tables.hasNext())
			{
				String table = (String) tables.next();
				
				primaryKeyList.clear();
				primaryKeyColumnSet.clear();
				
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
				
				Thread inactiveExecutor = new Thread(new StatementExecutor(inactiveStatement, sql));
				inactiveExecutor.start();
				
				Thread activeExecutor = new Thread(new StatementExecutor(activeStatement, sql));
				activeExecutor.start();
				
				try
				{
					inactiveExecutor.join();
				}
				catch (InterruptedException e)
				{
					throw new SQLException("Execution of " + sql + " on inactive database was interrupted.");
				}

				try
				{
					activeExecutor.join();
				}
				catch (InterruptedException e)
				{
					throw new SQLException("Execution of " + sql + " on active database was interrupted.");
				}
				
				ResultSet inactiveResultSet = inactiveStatement.getResultSet();

				if (inactiveResultSet == null)
				{
					throw inactiveStatement.getWarnings();
				}
				
				ResultSet activeResultSet = activeStatement.getResultSet();
				
				if (activeResultSet == null)
				{
					throw activeStatement.getWarnings();
				}
				
				primaryKeys = primaryKeyList.iterator();
				
				while (primaryKeys.hasNext())
				{
					String primaryKey = (String) primaryKeys.next();
					primaryKeyColumnSet.add(new Integer(activeResultSet.findColumn(primaryKey)));
				}
				
				boolean hasActiveResults = activeResultSet.next();
				boolean hasInactiveResults = inactiveResultSet.next();

				ResultSetMetaData resultSetMetaData = activeResultSet.getMetaData();
				int columns = resultSetMetaData.getColumnCount();
				
				int insertCount = 0;
				int updateCount = 0;
				int deleteCount = 0;
				
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
						Iterator primaryKeyColumns = primaryKeyColumnSet.iterator();
						
						while (primaryKeyColumns.hasNext())
						{
							Integer primaryKeyColumn = (Integer) primaryKeyColumns.next();
							int column = primaryKeyColumn.intValue();
							
							Comparable activeObject = (Comparable) activeResultSet.getObject(column);
							Object inactiveObject = inactiveResultSet.getObject(column);
							
							compare = activeObject.compareTo(inactiveObject);
							
							if (compare != 0)
							{
								break;
							}
						}
					}
					
					if (compare > 0)
					{
						inactiveResultSet.deleteRow();
						
						deleteCount += 1;
					}
					else if (compare < 0)
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
						
						insertCount += 1;
					}
					else // if (compare == 0)
					{
						boolean updated = false;
						
						for (int i = 1; i <= columns; ++i)
						{
							if (!primaryKeyColumnSet.contains(new Integer(i)))
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
										inactiveResultSet.updateObject(i, activeObject);
										
										updated = true;
									}
								}
							}
						}
						
						if (updated)
						{
							inactiveResultSet.updateRow();
							
							updateCount += 1;
						}
					}
					
					if (hasActiveResults && (compare <= 0))
					{
						hasActiveResults = activeResultSet.next();
					}
					
					if (hasInactiveResults && (compare >= 0))
					{
						hasInactiveResults = inactiveResultSet.next();
					}
				}
				
				log.info("Inserted " + insertCount + " rows into " + table);
				log.info("Deleted " + deleteCount + " rows from " + table);
				log.info("Updated " + updateCount + " rows in " + table);
				
				inactiveStatement.close();
				activeStatement.close();
				
				inactiveConnection.commit();
			}

			log.info("Database synchronization completed successfully");
		}
		finally
		{
			if ((inactiveConnection != null) && !inactiveConnection.isClosed())
			{
				try
				{
					inactiveConnection.rollback();
					
					Iterator foreignKeys = foreignKeyList.iterator();
					
					while (foreignKeys.hasNext())
					{
						ForeignKey foreignKey = (ForeignKey) foreignKeys.next();
						String sql = foreignKey.formatSQL(descriptor.getCreateForeignKeySQL());
						
						log.info("Recreating foreign key: " + sql);
						
						Statement statement = inactiveConnection.createStatement();
						statement.execute(sql);
						statement.close();
					}

					inactiveConnection.commit();
					inactiveConnection.close();
				}
				catch (SQLException e)
				{
					log.warn("Failed to close connection of inactive database", e);
				}
			}

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
		}
	}
}
