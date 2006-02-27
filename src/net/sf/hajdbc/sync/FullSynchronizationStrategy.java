/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
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
package net.sf.hajdbc.sync;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.util.concurrent.DaemonThreadFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Database-independent synchronization strategy that only updates differences between two databases.
 * This strategy is best used when there are <em>many</em> differences between the active database and the inactive database (i.e. very much out of sync).
 * The following algorithm is used:
 * <ol>
 *  <li>Drop the foreign keys on the inactive database (to avoid integrity constraint violations)</li>
 *  <li>For each database table:
 *   <ol>
 *    <li>Delete all rows in the inactive database table</li>
 *    <li>Query all rows on the active database table</li>
 *    <li>For each row in active database table:
 *     <ol>
 *      <li>Insert new row into inactive database table</li>
 *     </ol>
 *    </li>
 *   </ol>
 *  </li>
 *  <li>Re-create the foreign keys on the inactive database</li>
 * </ol>
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class FullSynchronizationStrategy implements SynchronizationStrategy
{
	private static Log log = LogFactory.getLog(FullSynchronizationStrategy.class);

	private ExecutorService executor = Executors.newSingleThreadExecutor(DaemonThreadFactory.getInstance());
	private int maxBatchSize = 100;
	private int fetchSize = 0;
	
	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#synchronize(Connection, Connection, Map, Dialect)
	 */
	public void synchronize(Connection inactiveConnection, Connection activeConnection, Map<String, List<String>> schemaMap, Dialect dialect) throws SQLException
	{
		inactiveConnection.setAutoCommit(true);
		
		DatabaseMetaData metaData = inactiveConnection.getMetaData();
		
		Statement statement = inactiveConnection.createStatement();
		
		// Drop foreign keys from the inactive database
		for (ForeignKeyConstraint key: ForeignKeyConstraint.collect(inactiveConnection, schemaMap))
		{
			String sql = dialect.getDropForeignKeyConstraintSQL(metaData, key.getName(), key.getSchema(), key.getTable());
			
			log.debug(sql);
			
			statement.addBatch(sql);
		}
		
		statement.executeBatch();
		statement.clearBatch();
		
		inactiveConnection.setAutoCommit(false);
		
		try
		{
			for (Map.Entry<String, List<String>> schemaMapEntry: schemaMap.entrySet())
			{
				String schema = schemaMapEntry.getKey();
				
				for (String table: schemaMapEntry.getValue())
				{
					String qualifiedTable = dialect.qualifyTable(metaData, schema, table);
				
					final String selectSQL = "SELECT * FROM " + qualifiedTable;
					
					final Statement selectStatement = activeConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
					selectStatement.setFetchSize(this.fetchSize);
					
					Callable<ResultSet> callable = new Callable<ResultSet>()
					{
						public ResultSet call() throws SQLException
						{
							return selectStatement.executeQuery(selectSQL);
						}
					};
		
					Future<ResultSet> future = this.executor.submit(callable);
					
					String deleteSQL = dialect.getTruncateTableSQL(metaData, schema, table);
		
					log.debug(deleteSQL);
					
					Statement deleteStatement = inactiveConnection.createStatement();
		
					int deletedRows = deleteStatement.executeUpdate(deleteSQL);
					
					log.info(Messages.getMessage(Messages.DELETE_COUNT, deletedRows, qualifiedTable));
					
					deleteStatement.close();
					
					ResultSet resultSet = future.get();
					
					StringBuilder insertSQL = new StringBuilder("INSERT INTO ").append(qualifiedTable).append(" (");
		
					ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
					
					int columns = resultSetMetaData.getColumnCount();
					
					for (int i = 1; i <= columns; ++i)
					{
						if (i > 1)
						{
							insertSQL.append(", ");
						}
						
						insertSQL.append(dialect.quote(metaData, resultSetMetaData.getColumnName(i)));
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
					
					log.debug(insertSQL);
					
					PreparedStatement insertStatement = inactiveConnection.prepareStatement(insertSQL.toString());
					int statementCount = 0;
					
					while (resultSet.next())
					{
						for (int i = 1; i <= columns; ++i)
						{
							Object object = resultSet.getObject(i);
							int type = resultSetMetaData.getColumnType(i);
							
							if (resultSet.wasNull())
							{
								insertStatement.setNull(i, type);
							}
							else
							{
								insertStatement.setObject(i, object, type);
							}
						}
						
						insertStatement.addBatch();
						statementCount += 1;
						
						if ((statementCount % this.maxBatchSize) == 0)
						{
							insertStatement.executeBatch();
							insertStatement.clearBatch();
						}
						
						insertStatement.clearParameters();
					}
		
					if ((statementCount % this.maxBatchSize) > 0)
					{
						insertStatement.executeBatch();
					}
		
					log.info(Messages.getMessage(Messages.INSERT_COUNT, statementCount, qualifiedTable));
					
					insertStatement.close();
					selectStatement.close();
					
					inactiveConnection.commit();
				}
			}
		}
		catch (InterruptedException e)
		{
			this.rollback(inactiveConnection);

			throw new net.sf.hajdbc.SQLException(e);
		}
		catch (ExecutionException e)
		{
			this.rollback(inactiveConnection);

			throw new net.sf.hajdbc.SQLException(e.getCause());
		}
		catch (SQLException e)
		{
			this.rollback(inactiveConnection);
			
			throw e;
		}
		
		inactiveConnection.setAutoCommit(true);

		// Collect foreign keys from active database and create them on inactive database
		for (ForeignKeyConstraint key: ForeignKeyConstraint.collect(activeConnection, schemaMap))
		{
			String sql = dialect.getCreateForeignKeyConstraintSQL(metaData, key.getName(), key.getSchema(), key.getTable(), key.getColumn(), key.getForeignSchema(), key.getForeignTable(), key.getForeignColumn());
			
			log.debug(sql);
			
			statement.addBatch(sql);
		}
		
		statement.executeBatch();
		statement.close();
	}
	
	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#requiresTableLocking()
	 */
	public boolean requiresTableLocking()
	{
		return true;
	}
	
	private void rollback(Connection connection)
	{
		try
		{
			connection.rollback();
			connection.setAutoCommit(true);
		}
		catch (java.sql.SQLException e)
		{
			log.warn(e.getMessage(), e);
		}
	}

	/**
	 * @return the fetchSize.
	 */
	public int getFetchSize()
	{
		return this.fetchSize;
	}

	/**
	 * @param fetchSize the fetchSize to set.
	 */
	public void setFetchSize(int fetchSize)
	{
		this.fetchSize = fetchSize;
	}
	
	/**
	 * @return the maxBatchSize.
	 */
	public int getMaxBatchSize()
	{
		return this.maxBatchSize;
	}

	/**
	 * @param maxBatchSize the maxBatchSize to set.
	 */
	public void setMaxBatchSize(int maxBatchSize)
	{
		this.maxBatchSize = maxBatchSize;
	}
}
