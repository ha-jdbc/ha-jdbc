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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
public class CleanDatabaseSynchronizationStrategy implements DatabaseSynchronizationStrategy
{
	private static final int MAX_BATCH_SIZE = 50;
	
	private static Log log = LogFactory.getLog(CleanDatabaseSynchronizationStrategy.class);

	/**
	 * @see net.sf.hajdbc.DatabaseActivationStrategy#activate(net.sf.hajdbc.DatabaseCluster, net.sf.hajdbc.Database)
	 */
	public void synchronize(DatabaseCluster databaseCluster, Database database) throws SQLException
	{
		DatabaseClusterDescriptor descriptor = databaseCluster.getDescriptor();
		Connection activeConnection = null;
		Connection inactiveConnection = null;
		
		List foreignKeyList = new LinkedList();
		Map indexMap = new HashMap();
		
		try
		{
			Database activeDatabase = databaseCluster.nextDatabase();
			
			inactiveConnection = database.connect(databaseCluster.getDatabaseConnector().getSQLObject(database));
			inactiveConnection.setAutoCommit(false);
			
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
					String sql = foreignKey.formatSQL(descriptor.getDropForeignKeySQL());
					
					log.info("Dropping foreign key: " + sql);
					
					Statement statement = inactiveConnection.createStatement();
					statement.execute(sql);
					statement.close();
					
					foreignKeyList.add(foreignKey);
				}
				
				resultSet.close();
				
				resultSet = databaseMetaData.getIndexInfo(null, null, table, false, true);
				
				while (resultSet.next())
				{
					if (resultSet.getBoolean("NON_UNIQUE"))
					{
						String name = resultSet.getString("INDEX_NAME");
						String column = resultSet.getString("COLUMN_NAME");
						
						Index index = (Index) indexMap.get(name);
						
						if (index == null)
						{
							index = new Index(name, table);
							
							String sql = index.formatSQL(descriptor.getDropIndexSQL());
							
							log.info("Dropping non-unique index: " + sql);
							
							Statement statement = inactiveConnection.createStatement();
							statement.execute(index.formatSQL(sql));
							statement.close();
							
							indexMap.put(name, index);
						}
						
						index.addColumn(column);
					}
				}
				
				resultSet.close();
			}
			
			inactiveConnection.commit();
			
			activeConnection = activeDatabase.connect(databaseCluster.getDatabaseConnector().getSQLObject(activeDatabase));
			activeConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			activeConnection.setAutoCommit(false);
			
			tables = tableList.iterator();
			
			while (tables.hasNext())
			{
				String table = (String) tables.next();

				String deleteSQL = "DELETE FROM " + table;
				String selectSQL = "SELECT * FROM " + table;

				log.info("Deleting: " + deleteSQL);
				
				Statement deleteStatement = inactiveConnection.createStatement();
				Statement selectStatement = activeConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				
				Thread deleteExecutor = new Thread(new StatementExecutor(deleteStatement, deleteSQL));
				deleteExecutor.start();

				Thread selectExecutor = new Thread(new StatementExecutor(selectStatement, selectSQL));
				selectExecutor.start();
				
				try
				{
					deleteExecutor.join();
				}
				catch (InterruptedException e)
				{
					throw new SQLException("Execution of " + deleteSQL + " was interrupted.");
				}

				try
				{
					selectExecutor.join();
				}
				catch (InterruptedException e)
				{
					throw new SQLException("Execution of " + selectSQL + " was interrupted.");
				}
				
				int deletedRows = deleteStatement.getUpdateCount();
				
				if (deletedRows < 0)
				{
					throw deleteStatement.getWarnings();
				}
				
				log.info("Deleted " + deletedRows + " rows from " + table);
				
				deleteStatement.close();
				
				resultSet = selectStatement.getResultSet();
				
				if (resultSet == null)
				{
					throw selectStatement.getWarnings();
				}
				
				ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
				
				int columns = resultSetMetaData.getColumnCount();

				StringBuffer insertSQL = new StringBuffer("INSERT INTO ").append(table).append(" (");

				for (int i = 1; i <= columns; ++i)
				{
					if (i > 1)
					{
						insertSQL.append(", ");
					}
					
					insertSQL.append(resultSetMetaData.getColumnName(i));
				}
				
				insertSQL.append(") VALUES (");
				
				for (int i = 1; i <= columns; ++i)
				{
					if (i > 1)
					{
						insertSQL.append(", ");
					}
					
					insertSQL.append("?");
				}
				
				insertSQL.append(")");
				
				log.info("Inserting: " + insertSQL);
				PreparedStatement insertStatement = inactiveConnection.prepareStatement(insertSQL.toString());
				int statementCount = 0;
				
				while (resultSet.next())
				{
					insertStatement.clearParameters();
					
					for (int i = 1; i <= columns; ++i)
					{
						Object object = resultSet.getObject(i);
						
						if (resultSet.wasNull())
						{
							insertStatement.setNull(i, resultSetMetaData.getColumnType(i));
						}
						else
						{
							insertStatement.setObject(i, object);
						}
					}
					
					insertStatement.addBatch();
					statementCount += 1;
					
					if ((statementCount % MAX_BATCH_SIZE) == 0)
					{
						insertStatement.executeBatch();
					}
				}

				if (statementCount > 0)
				{
					insertStatement.executeBatch();
				}

				log.info("Inserted " + statementCount + " rows into " + table);
				
				insertStatement.close();
				selectStatement.close();
				
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
					
					Iterator indexes = indexMap.values().iterator();
					
					while (indexes.hasNext())
					{
						Index index = (Index) indexes.next();
						String sql = index.formatSQL(descriptor.getCreateIndexSQL());
						
						log.info("Recreating non-unique index: " + sql);
						
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
					activeConnection.rollback();
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
