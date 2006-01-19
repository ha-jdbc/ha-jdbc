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
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.util.concurrent.DaemonThreadFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
public class DifferentialSynchronizationStrategy extends AbstractSynchronizationStrategy
{
	private static Log log = LogFactory.getLog(DifferentialSynchronizationStrategy.class);

	private ExecutorService executor = Executors.newSingleThreadExecutor(new DaemonThreadFactory());
	
	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#synchronize(Connection, Connection, Map, Dialect)
	 */
	public void synchronize(Connection inactiveConnection, Connection activeConnection, Map<String, List<String>> schemaMap, Dialect dialect) throws SQLException
	{
		inactiveConnection.setAutoCommit(true);
		
		DatabaseMetaData metaData = inactiveConnection.getMetaData();
		Statement statement = inactiveConnection.createStatement();
		
		// Drop foreign key constraints on the inactive database
		for (ForeignKeyConstraint key: ForeignKeyConstraint.collect(inactiveConnection, schemaMap))
		{
			statement.addBatch(dialect.getDropForeignKeyConstraintSQL(metaData, key.getName(), key.getSchema(), key.getTable()));
		}

		statement.executeBatch();
		statement.clearBatch();
		
		inactiveConnection.setAutoCommit(false);
		
		DatabaseMetaData databaseMetaData = inactiveConnection.getMetaData();
		
		Map<Short, String> primaryKeyColumnMap = new TreeMap<Short, String>();
		Set<Integer> primaryKeyColumnIndexSet = new LinkedHashSet<Integer>();
		
		try
		{
			for (Map.Entry<String, List<String>> schemaMapEntry: schemaMap.entrySet())
			{
				String schema = schemaMapEntry.getKey();
				
				for (String table: schemaMapEntry.getValue())
				{
					String qualifiedTable = dialect.qualifyTable(metaData, schema, table);

					primaryKeyColumnMap.clear();
					primaryKeyColumnIndexSet.clear();
					
					// Fetch primary keys of this table
					ResultSet primaryKeyResultSet = databaseMetaData.getPrimaryKeys(null, schema, table);
					String primaryKeyName = null;
					
					while (primaryKeyResultSet.next())
					{
						String name = primaryKeyResultSet.getString("COLUMN_NAME");
						short position = primaryKeyResultSet.getShort("KEY_SEQ");
		
						primaryKeyColumnMap.put(position, name);
						
						primaryKeyName = primaryKeyResultSet.getString("PK_NAME");
					}
					
					primaryKeyResultSet.close();
					
					if (primaryKeyColumnMap.isEmpty())
					{
						throw new SQLException(Messages.getMessage(Messages.PRIMARY_KEY_REQUIRED, this.getClass().getName(), table));
					}
					
					// Drop unique constraints on the current table
					for (UniqueConstraint constraint: UniqueConstraint.collect(inactiveConnection, schema, table, primaryKeyName))
					{
						statement.addBatch(dialect.getDropUniqueConstraintSQL(metaData, constraint.getName(), constraint.getSchema(), constraint.getTable()));
					}
					
					statement.executeBatch();
					statement.clearBatch();
					
					// Retrieve table rows in primary key order
					StringBuilder builder = new StringBuilder("SELECT * FROM ").append(qualifiedTable).append(" ORDER BY ");
					
					Iterator<String> primaryKeyColumns = primaryKeyColumnMap.values().iterator();
					
					while (primaryKeyColumns.hasNext())
					{
						builder.append(dialect.quote(metaData, primaryKeyColumns.next()));
						
						if (primaryKeyColumns.hasNext())
						{
							builder.append(", ");
						}
					}
					
					final String sql = builder.toString();
					
					final Statement inactiveStatement = inactiveConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
					inactiveStatement.setFetchSize(this.fetchSize);
		
					if (log.isDebugEnabled())
					{
						log.debug(sql);
					}
					
					Callable<ResultSet> callable = new Callable<ResultSet>()
					{
						public ResultSet call() throws java.sql.SQLException
						{
							return inactiveStatement.executeQuery(sql);
						}
					};
		
					Future<ResultSet> future = this.executor.submit(callable);
					
					Statement activeStatement = activeConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
					activeStatement.setFetchSize(this.fetchSize);
					
					ResultSet activeResultSet = activeStatement.executeQuery(sql);

					ResultSet inactiveResultSet = future.get();
					
					// Construct DELETE SQL
					StringBuilder deleteSQL = new StringBuilder("DELETE FROM ").append(qualifiedTable).append(" WHERE ");
					
					// Create set of primary key columns
					primaryKeyColumns = primaryKeyColumnMap.values().iterator();
					
					while (primaryKeyColumns.hasNext())
					{
						String primaryKeyColumn = primaryKeyColumns.next();
						
						primaryKeyColumnIndexSet.add(activeResultSet.findColumn(primaryKeyColumn));
						
						deleteSQL.append(dialect.quote(metaData, primaryKeyColumn)).append(" = ?");
						
						if (primaryKeyColumns.hasNext())
						{
							deleteSQL.append(" AND ");
						}
					}
		
					PreparedStatement deleteStatement = inactiveConnection.prepareStatement(deleteSQL.toString());
					
					ResultSetMetaData resultSetMetaData = activeResultSet.getMetaData();
					int columns = resultSetMetaData.getColumnCount();
					int[] types = new int[columns + 1];
					
					// Construct INSERT SQL
					StringBuilder insertSQL = new StringBuilder("INSERT INTO ").append(qualifiedTable).append(" (");
					
					for (int i = 1; i <= columns; ++i)
					{
						types[i] = resultSetMetaData.getColumnType(i);
						
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
		
					PreparedStatement insertStatement = inactiveConnection.prepareStatement(insertSQL.toString());
					
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
							for (int column: primaryKeyColumnIndexSet)
							{
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
							deleteStatement.clearParameters();
							
							int index = 1;
							
							for (int column: primaryKeyColumnIndexSet)
							{
								deleteStatement.setObject(index, inactiveResultSet.getObject(column), types[column]);
								
								index += 1;
							}
							
							deleteStatement.addBatch();
							
							deleteCount += 1;
						}
						else if (compare < 0)
						{
							insertStatement.clearParameters();
		
							for (int i = 1; i <= columns; ++i)
							{
								Object object = activeResultSet.getObject(i);
								
								if (activeResultSet.wasNull())
								{
									insertStatement.setNull(i, types[i]);
								}
								else
								{
									insertStatement.setObject(i, object, types[i]);
								}
							}
							
							insertStatement.addBatch();
							
							insertCount += 1;
						}
						else // if (compare == 0)
						{
							boolean updated = false;
							
							for (int i = 1; i <= columns; ++i)
							{
								if (!primaryKeyColumnIndexSet.contains(i))
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
										if (inactiveResultSet.wasNull() || !equals(activeObject, inactiveObject))
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
					
					inactiveStatement.close();
					activeStatement.close();
					
					// Collect unique constraints on this table from the active database and re-create them on the inactive database
					for (UniqueConstraint constraint: UniqueConstraint.collect(activeConnection, schema, table, primaryKeyName))
					{
						statement.addBatch(dialect.getCreateUniqueConstraintSQL(metaData, constraint.getName(), constraint.getSchema(), constraint.getTable(), constraint.getColumnList()));
					}
					
					statement.executeBatch();
					statement.clearBatch();
					
					inactiveConnection.commit();
					
					log.info(Messages.getMessage(Messages.INSERT_COUNT, insertCount, qualifiedTable));
					log.info(Messages.getMessage(Messages.UPDATE_COUNT, updateCount, qualifiedTable));
					log.info(Messages.getMessage(Messages.DELETE_COUNT, deleteCount, qualifiedTable));			
				}
			}
	
			inactiveConnection.setAutoCommit(true);

			// Collect foreign key constraints from the active database and create them on the inactive database
			for (ForeignKeyConstraint key: ForeignKeyConstraint.collect(activeConnection, schemaMap))
			{
				statement.addBatch(dialect.getCreateForeignKeyConstraintSQL(metaData, key.getName(), key.getSchema(), key.getTable(), key.getColumn(), key.getForeignSchema(), key.getForeignTable(), key.getForeignColumn()));
			}
			
			statement.executeBatch();
			statement.close();
		}
		catch (ExecutionException e)
		{
			throw new net.sf.hajdbc.SQLException(e.getCause());
		}
		catch (InterruptedException e)
		{
			throw new net.sf.hajdbc.SQLException(e);
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
}
