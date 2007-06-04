/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.SynchronizationContext;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.TableProperties;
import net.sf.hajdbc.UniqueConstraint;
import net.sf.hajdbc.util.SQLExceptionFactory;
import net.sf.hajdbc.util.Strings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Database-independent synchronization strategy that only updates differences between two databases.
 * This strategy is best used when there are <em>few</em> differences between the active database and the inactive database (i.e. barely out of sync).
 * The following algorithm is used:
 * <ol>
 *  <li>Drop the foreign keys on the inactive database (to avoid integrity constraint violations)</li>
 *  <li>For each database table:
 *   <ol>
 *    <li>Drop the unique constraints on the table (to avoid integrity constraint violations)</li>
 *    <li>Find the primary key(s) of the table</li>
 *    <li>Query all rows in the inactive database table, sorting by the primary key(s)</li>
 *    <li>Query all rows on the active database table</li>
 *    <li>For each row in table:
 *     <ol>
 *      <li>If primary key of the rows are the same, determine whether or not row needs to be updated</li>
 *      <li>Otherwise, determine whether row should be deleted, or a new row is to be inserted</li>
 *     </ol>
 *    </li>
 *    <li>Re-create the unique constraints on the table (to avoid integrity constraint violations)</li>
 *   </ol>
 *  </li>
 *  <li>Re-create the foreign keys on the inactive database</li>
 *  <li>Synchronize sequences</li>
 * </ol>
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class DifferentialSynchronizationStrategy implements SynchronizationStrategy
{
	private static Logger logger = LoggerFactory.getLogger(DifferentialSynchronizationStrategy.class);

	private int fetchSize = 0;
	private int maxBatchSize = 100;
	
	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#cleanup(net.sf.hajdbc.SynchronizationContext)
	 */
	public <D> void cleanup(SynchronizationContext<D> context)
	{
		SynchronizationSupport.unlock(context);
	}

	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#prepare(net.sf.hajdbc.SynchronizationContext)
	 */
	public <D> void prepare(SynchronizationContext<D> context) throws SQLException
	{
		SynchronizationSupport.lock(context);
	}
	
	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#synchronize(net.sf.hajdbc.SynchronizationContext)
	 */
	public <D> void synchronize(SynchronizationContext<D> context) throws SQLException
	{
		Map<Short, String> primaryKeyColumnMap = new TreeMap<Short, String>();
		Set<Integer> primaryKeyColumnIndexSet = new LinkedHashSet<Integer>();
		
		Connection sourceConnection = context.getConnection(context.getSourceDatabase());
		Connection targetConnection = context.getConnection(context.getTargetDatabase());

		Dialect dialect = context.getDialect();

		ExecutorService executor = context.getExecutor();
		
		targetConnection.setAutoCommit(true);
		
		SynchronizationSupport.dropForeignKeys(context);
		
		try
		{
			for (TableProperties table: context.getDatabaseMetaDataCache().getDatabaseProperties(targetConnection).getTables())
			{
				SynchronizationSupport.dropUniqueConstraints(context, table);
				
				targetConnection.setAutoCommit(false);
				
				primaryKeyColumnMap.clear();
				primaryKeyColumnIndexSet.clear();
				
				String tableName = table.getName();
				
				UniqueConstraint primaryKey = table.getPrimaryKey();
				
				if (primaryKey == null)
				{
					throw new SQLException(Messages.getMessage(Messages.PRIMARY_KEY_REQUIRED, this.getClass().getName(), tableName));
				}
				
				List<String> primaryKeyColumnList = primaryKey.getColumnList();
				
				Collection<String> columns = table.getColumns();
				
				// List of colums for select statement - starting with primary key
				List<String> columnList = new ArrayList<String>(columns.size());
				
				columnList.addAll(primaryKeyColumnList);
				
				for (String column: columns)
				{
					if (!primaryKeyColumnList.contains(column))
					{
						columnList.add(column);
					}
				}
				
				List<String> nonPrimaryKeyColumnList = columnList.subList(primaryKeyColumnList.size(), columnList.size());
				
				String commaDelimitedColumns = Strings.join(columnList, ", ");
				
				// Retrieve table rows in primary key order
				final String selectSQL = "SELECT " + commaDelimitedColumns + " FROM " + tableName + " ORDER BY " + Strings.join(primaryKeyColumnList, ", ");
				
				final Statement targetStatement = targetConnection.createStatement();

				targetStatement.setFetchSize(this.fetchSize);
	
				logger.debug(selectSQL);
				
				Callable<ResultSet> callable = new Callable<ResultSet>()
				{
					public ResultSet call() throws SQLException
					{
						return targetStatement.executeQuery(selectSQL);
					}
				};
	
				Future<ResultSet> future = executor.submit(callable);
				
				Statement sourceStatement = sourceConnection.createStatement();
				sourceStatement.setFetchSize(this.fetchSize);
				
				ResultSet sourceResultSet = sourceStatement.executeQuery(selectSQL);

				ResultSet inactiveResultSet = future.get();
				
				String primaryKeyWhereClause = " WHERE " + Strings.join(primaryKeyColumnList, " = ? AND ") + " = ?";
				
				// Construct DELETE SQL
				String deleteSQL = "DELETE FROM " + tableName + primaryKeyWhereClause;
				
				logger.debug(deleteSQL);
				
				PreparedStatement deleteStatement = targetConnection.prepareStatement(deleteSQL);
				
				String[] parameters = new String[columnList.size()];
				Arrays.fill(parameters, "?");
				
				// Construct INSERT SQL
				String insertSQL = "INSERT INTO " + tableName + " (" + commaDelimitedColumns + ") VALUES (" + Strings.join(Arrays.asList(parameters), ", ") + ")";
				
				logger.debug(insertSQL);
				
				PreparedStatement insertStatement = targetConnection.prepareStatement(insertSQL);
				
				// Construct UPDATE SQL
				String updateSQL = "UPDATE " + tableName + " SET " + Strings.join(nonPrimaryKeyColumnList, " = ?, ") + " = ?" + primaryKeyWhereClause;
				
				logger.debug(updateSQL);
				
				PreparedStatement updateStatement = targetConnection.prepareStatement(updateSQL);
				
				boolean hasMoreActiveResults = sourceResultSet.next();
				boolean hasMoreInactiveResults = inactiveResultSet.next();
				
				int insertCount = 0;
				int updateCount = 0;
				int deleteCount = 0;
				
				while (hasMoreActiveResults || hasMoreInactiveResults)
				{
					int compare = 0;
					
					if (!hasMoreActiveResults)
					{
						compare = 1;
					}
					else if (!hasMoreInactiveResults)
					{
						compare = -1;
					}
					else
					{
						for (int i = 1; i <= primaryKeyColumnList.size(); ++i)
						{
							Object activeObject = sourceResultSet.getObject(i);
							Object inactiveObject = inactiveResultSet.getObject(i);
							
							// We assume that the primary keys column types are Comparable
							compare = this.compare(activeObject, inactiveObject);
							
							if (compare != 0)
							{
								break;
							}
						}
					}
					
					if (compare > 0)
					{
						deleteStatement.clearParameters();
						
						for (int i = 1; i <= primaryKeyColumnList.size(); ++i)
						{
							int type = dialect.getColumnType(table.getColumnProperties(columnList.get(i - 1)));
							
							deleteStatement.setObject(i, inactiveResultSet.getObject(i), type);
						}
						
						deleteStatement.addBatch();
						
						deleteCount += 1;
						
						if ((deleteCount % this.maxBatchSize) == 0)
						{
							deleteStatement.executeBatch();
							deleteStatement.clearBatch();
						}
					}
					else if (compare < 0)
					{
						insertStatement.clearParameters();

						for (int i = 1; i <= columnList.size(); ++i)
						{
							int type = dialect.getColumnType(table.getColumnProperties(columnList.get(i - 1)));

							Object object = SynchronizationSupport.getObject(sourceResultSet, i, type);
							
							if (sourceResultSet.wasNull())
							{
								insertStatement.setNull(i, type);
							}
							else
							{
								insertStatement.setObject(i, object, type);
							}
						}
						
						insertStatement.addBatch();
						
						insertCount += 1;
						
						if ((insertCount % this.maxBatchSize) == 0)
						{
							insertStatement.executeBatch();
							insertStatement.clearBatch();
						}
					}
					else // if (compare == 0)
					{
						updateStatement.clearParameters();
						
						boolean updated = false;
						
						for (int i = primaryKeyColumnList.size() + 1; i <= columnList.size(); ++i)
						{
							int type = dialect.getColumnType(table.getColumnProperties(columnList.get(i - 1)));
							
							Object activeObject = SynchronizationSupport.getObject(sourceResultSet, i, type);
							Object inactiveObject = SynchronizationSupport.getObject(inactiveResultSet, i, type);
							
							int index = i - primaryKeyColumnList.size();
							
							if (sourceResultSet.wasNull())
							{
								updateStatement.setNull(index, type);
								
								updated |= !inactiveResultSet.wasNull();
							}
							else
							{
								updateStatement.setObject(index, activeObject, type);
								
								updated |= inactiveResultSet.wasNull();
								updated |= !equals(activeObject, inactiveObject);
							}
						}
						
						if (updated)
						{
							for (int i = 1; i <= primaryKeyColumnList.size(); ++i)
							{
								int type = dialect.getColumnType(table.getColumnProperties(columnList.get(i - 1)));
								
								updateStatement.setObject(i + nonPrimaryKeyColumnList.size(), inactiveResultSet.getObject(i), type);
							}
							
							updateStatement.addBatch();
							
							updateCount += 1;
							
							if ((updateCount % this.maxBatchSize) == 0)
							{
								updateStatement.executeBatch();
								updateStatement.clearBatch();
							}
						}
					}
					
					if (hasMoreActiveResults && (compare <= 0))
					{
						hasMoreActiveResults = sourceResultSet.next();
					}
					
					if (hasMoreInactiveResults && (compare >= 0))
					{
						hasMoreInactiveResults = inactiveResultSet.next();
					}
				}
				
				if ((deleteCount % this.maxBatchSize) > 0)
				{
					deleteStatement.executeBatch();
				}
				
				deleteStatement.close();
				
				if ((insertCount % this.maxBatchSize) > 0)
				{
					insertStatement.executeBatch();
				}
				
				insertStatement.close();
				
				if ((updateCount % this.maxBatchSize) > 0)
				{
					updateStatement.executeBatch();
				}
				
				updateStatement.close();
				
				targetStatement.close();
				sourceStatement.close();
				
				targetConnection.commit();

				targetConnection.setAutoCommit(true);
				
				SynchronizationSupport.restoreUniqueConstraints(context, table);
				
				logger.info(Messages.getMessage(Messages.INSERT_COUNT, insertCount, tableName));
				logger.info(Messages.getMessage(Messages.UPDATE_COUNT, updateCount, tableName));
				logger.info(Messages.getMessage(Messages.DELETE_COUNT, deleteCount, tableName));			
			}
		}
		catch (ExecutionException e)
		{
			SynchronizationSupport.rollback(targetConnection);
			
			throw SQLExceptionFactory.createSQLException(e.getCause());
		}
		catch (InterruptedException e)
		{
			SynchronizationSupport.rollback(targetConnection);
			
			throw SQLExceptionFactory.createSQLException(e.getCause());
		}
		catch (SQLException e)
		{
			SynchronizationSupport.rollback(targetConnection);
			
			throw e;
		}
		
		SynchronizationSupport.restoreForeignKeys(context);
		
		if (dialect.supportsSequences())
		{
			SynchronizationSupport.synchronizeSequences(context);
		}
	}

	private boolean equals(Object object1, Object object2)
	{
		if (byte[].class.isInstance(object1) && byte[].class.isInstance(object2))
		{
			byte[] bytes1 = (byte[]) object1;
			byte[] bytes2 = (byte[]) object2;
			
			if (bytes1.length != bytes2.length)
			{
				return false;
			}
			
			return Arrays.equals(bytes1, bytes2);
		}
		
		return object1.equals(object2);
	}
	
	@SuppressWarnings("unchecked")
	private int compare(Object object1, Object object2)
	{
		return Comparable.class.cast(object1).compareTo(object2);
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
	 * @return Returns the maxBatchSize.
	 */
	public int getMaxBatchSize()
	{
		return this.maxBatchSize;
	}

	/**
	 * @param maxBatchSize The maxBatchSize to set.
	 */
	public void setMaxBatchSize(int maxBatchSize)
	{
		this.maxBatchSize = maxBatchSize;
	}
}
