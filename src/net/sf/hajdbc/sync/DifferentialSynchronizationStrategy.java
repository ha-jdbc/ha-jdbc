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
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import net.sf.hajdbc.DatabaseMetaDataCache;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.UniqueConstraint;
import net.sf.hajdbc.util.Strings;
import net.sf.hajdbc.util.concurrent.DaemonThreadFactory;

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
 * </ol>
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class DifferentialSynchronizationStrategy implements SynchronizationStrategy
{
	private static Logger logger = LoggerFactory.getLogger(DifferentialSynchronizationStrategy.class);

	private ExecutorService executor = Executors.newSingleThreadExecutor(DaemonThreadFactory.getInstance());
	private int fetchSize = 0;
	
	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#synchronize(Connection, Connection, Map, Dialect)
	 */
	public void synchronize(Connection inactiveConnection, Connection activeConnection, DatabaseMetaDataCache metaData, Dialect dialect) throws SQLException
	{
		inactiveConnection.setAutoCommit(true);
		
		Statement statement = inactiveConnection.createStatement();

		Map<String, Collection<String>> schemaMap = metaData.getTables();
		
		// Drop foreign key constraints on the inactive database
		for (Map.Entry<String, Collection<String>> schemaMapEntry: schemaMap.entrySet())
		{
			String schema = schemaMapEntry.getKey();
			
			for (String table: schemaMapEntry.getValue())
			{
				for (ForeignKeyConstraint constraint: metaData.getForeignKeyConstraints(schema, table))
				{
					String sql = dialect.getDropForeignKeyConstraintSQL(metaData, constraint);
					
					logger.debug(sql);
					
					statement.addBatch(sql);
				}
			}
		}
		
		statement.executeBatch();
		statement.clearBatch();
		
		Map<Short, String> primaryKeyColumnMap = new TreeMap<Short, String>();
		Set<Integer> primaryKeyColumnIndexSet = new LinkedHashSet<Integer>();
		
		inactiveConnection.setAutoCommit(false);
		
		try
		{
			for (Map.Entry<String, Collection<String>> schemaMapEntry: schemaMap.entrySet())
			{
				String schema = schemaMapEntry.getKey();
				
				for (String table: schemaMapEntry.getValue())
				{
					String qualifiedTable = metaData.getQualifiedTableForDML(schema, table);

					primaryKeyColumnMap.clear();
					primaryKeyColumnIndexSet.clear();
					
					UniqueConstraint primaryKey = metaData.getPrimaryKey(schema, table);
					
					if (primaryKey == null)
					{
						throw new SQLException(Messages.getMessage(Messages.PRIMARY_KEY_REQUIRED, this.getClass().getName(), table));
					}
					
					List<String> primaryKeyColumnList = primaryKey.getColumnList();
					
					Collection<UniqueConstraint> constraints = metaData.getUniqueConstraints(schema, table);
					
					constraints.remove(primaryKey);
					
 					// Drop unique constraints on the current table
					for (UniqueConstraint constraint: constraints)
					{
						String sql = dialect.getDropUniqueConstraintSQL(metaData, constraint);
						
						logger.debug(sql);
						
						statement.addBatch(sql);
					}
					
					statement.executeBatch();
					statement.clearBatch();
					
					Set<String> columnSet = metaData.getColumns(schema, table).keySet();
					
					// List of colums for select statement - starting with primary key
					List<String> columnList = new ArrayList<String>(columnSet);
					
					columnList.addAll(primaryKeyColumnList);
					
					for (String column: columnSet)
					{
						if (!primaryKeyColumnList.contains(column))
						{
							columnList.add(column);
						}
					}
					
					List<String> nonPrimaryKeyColumnList = columnList.subList(primaryKeyColumnList.size(), columnList.size());
					
					String commaDelimitedColumns = Strings.join(columnList, ",");
					
					// Retrieve table rows in primary key order
					final String selectSQL = "SELECT " + commaDelimitedColumns + " FROM " + qualifiedTable + " ORDER BY " + Strings.join(primaryKeyColumnList, ",");
					
					final Statement inactiveStatement = inactiveConnection.createStatement();

					inactiveStatement.setFetchSize(this.fetchSize);
		
					logger.debug(selectSQL);
					
					Callable<ResultSet> callable = new Callable<ResultSet>()
					{
						public ResultSet call() throws java.sql.SQLException
						{
							return inactiveStatement.executeQuery(selectSQL);
						}
					};
		
					Future<ResultSet> future = this.executor.submit(callable);
					
					Statement activeStatement = activeConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
					activeStatement.setFetchSize(this.fetchSize);
					
					ResultSet activeResultSet = activeStatement.executeQuery(selectSQL);

					ResultSet inactiveResultSet = future.get();
					
					String primaryKeyWhereClause = " WHERE " + Strings.join(primaryKeyColumnList, "=? AND ") + "=?";
					
					// Construct DELETE SQL
					String deleteSQL = "DELETE FROM " + qualifiedTable + primaryKeyWhereClause;
					
					logger.debug(deleteSQL.toString());
					
					PreparedStatement deleteStatement = inactiveConnection.prepareStatement(deleteSQL);
					
					String[] parameters = new String[columnList.size()];
					Arrays.fill(parameters, "?");
					
					// Construct INSERT SQL
					String insertSQL = "INSERT INTO " + qualifiedTable + " (" + commaDelimitedColumns + ") VALUES (" + Strings.join(Arrays.asList(parameters), ",") + ")";
					
					logger.debug(insertSQL);
					
					PreparedStatement insertStatement = inactiveConnection.prepareStatement(insertSQL);
					
					// Construct UPDATE SQL
					String updateSQL = "UPDATE " + qualifiedTable + " SET " + Strings.join(nonPrimaryKeyColumnList, "=?,") + "=?" + primaryKeyWhereClause;
					
					logger.debug(updateSQL);
					
					PreparedStatement updateStatement = inactiveConnection.prepareStatement(updateSQL);
					
					boolean hasMoreActiveResults = activeResultSet.next();
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
								Object activeObject = activeResultSet.getObject(i);
								Object inactiveObject = inactiveResultSet.getObject(i);
								
								// We assume that the primary keys column types are Comparable
								compare = Comparable.class.cast(activeObject).compareTo(inactiveObject);
								
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
								int type = dialect.getColumnType(metaData, schema, table, columnList.get(i - 1));
								
								deleteStatement.setObject(i, inactiveResultSet.getObject(i), type);
							}
							
							deleteStatement.addBatch();
							
							deleteCount += 1;
						}
						else if (compare < 0)
						{
							insertStatement.clearParameters();

							for (int i = 1; i <= columnList.size(); ++i)
							{
								Object object = activeResultSet.getObject(i);
								
								int type = dialect.getColumnType(metaData, schema, table, columnList.get(i - 1));
								
								if (activeResultSet.wasNull())
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
						}
						else // if (compare == 0)
						{
							updateStatement.clearParameters();
							
							boolean updated = false;
							
							for (int i = primaryKeyColumnList.size() + 1; i <= columnList.size(); ++i)
							{
								Object activeObject = activeResultSet.getObject(i);
								Object inactiveObject = inactiveResultSet.getObject(i);
								
								int type = dialect.getColumnType(metaData, schema, table, columnList.get(i - 1));
								
								int index = i - primaryKeyColumnList.size();
								
								if (activeResultSet.wasNull())
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
									int type = dialect.getColumnType(metaData, schema, table, columnList.get(i - 1));
									
									updateStatement.setObject(i + nonPrimaryKeyColumnList.size(), inactiveResultSet.getObject(i), type);
								}
								
								updateStatement.addBatch();
								
								updateCount += 1;
							}
						}
						
						if (hasMoreActiveResults && (compare <= 0))
						{
							hasMoreActiveResults = activeResultSet.next();
						}
						
						if (hasMoreInactiveResults && (compare >= 0))
						{
							hasMoreInactiveResults = inactiveResultSet.next();
						}
					}
					
					if (deleteCount > 0)
					{
						deleteStatement.executeBatch();
					}
					
					deleteStatement.close();
					
					if (insertCount > 0)
					{
						insertStatement.executeBatch();
					}
					
					insertStatement.close();
					
					if (updateCount > 0)
					{
						updateStatement.executeBatch();
					}
					
					updateStatement.close();
					
					inactiveStatement.close();
					activeStatement.close();
					
					// Collect unique constraints on this table from the active database and re-create them on the inactive database
					for (UniqueConstraint constraint: constraints)
					{
						String sql = dialect.getCreateUniqueConstraintSQL(metaData, constraint);
						
						logger.debug(sql);
						
						statement.addBatch(sql);
					}
					
					statement.executeBatch();
					statement.clearBatch();
					
					inactiveConnection.commit();
					
					logger.info(Messages.getMessage(Messages.INSERT_COUNT, insertCount, qualifiedTable));
					logger.info(Messages.getMessage(Messages.UPDATE_COUNT, updateCount, qualifiedTable));
					logger.info(Messages.getMessage(Messages.DELETE_COUNT, deleteCount, qualifiedTable));			
				}
			}
		}
		catch (ExecutionException e)
		{
			this.rollback(inactiveConnection);
			
			throw new net.sf.hajdbc.SQLException(e.getCause());
		}
		catch (InterruptedException e)
		{
			this.rollback(inactiveConnection);
			
			throw new net.sf.hajdbc.SQLException(e);
		}
		catch (SQLException e)
		{
			this.rollback(inactiveConnection);
			
			throw e;
		}
		
		inactiveConnection.setAutoCommit(true);

		// Collect foreign key constraints from the active database and create them on the inactive database
		for (Map.Entry<String, Collection<String>> schemaMapEntry: schemaMap.entrySet())
		{
			String schema = schemaMapEntry.getKey();
			
			for (String table: schemaMapEntry.getValue())
			{
				for (ForeignKeyConstraint constraint: metaData.getForeignKeyConstraints(schema, table))
				{
					String sql = dialect.getCreateForeignKeyConstraintSQL(metaData, constraint);
					
					logger.debug(sql);
					
					statement.addBatch(sql);
				}
			}
		}
		
		statement.executeBatch();
		statement.close();
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
			logger.warn(e.toString(), e);
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
}
