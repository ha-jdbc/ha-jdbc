/*
 * HA-JDBC: High-Availability JDBC
 * Copyright 2004-2009 Paul Ferraro
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc.sync;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.ExceptionType;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.cache.TableProperties;
import net.sf.hajdbc.cache.UniqueConstraint;
import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;
import net.sf.hajdbc.util.Objects;
import net.sf.hajdbc.util.Strings;

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
 */
public class DifferentialSynchronizationStrategy implements SynchronizationStrategy, Serializable
{
	private static final long serialVersionUID = -2785092229503649831L;

	private static Logger logger = LoggerFactory.getLogger(DifferentialSynchronizationStrategy.class);

	private int fetchSize = 0;
	private int maxBatchSize = 100;
	private Pattern versionPattern = null;
	
	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#synchronize(net.sf.hajdbc.sync.SynchronizationContext)
	 */
	@Override
	public <Z, D extends Database<Z>> void synchronize(SynchronizationContext<Z, D> context) throws SQLException
	{
		Connection sourceConnection = context.getConnection(context.getSourceDatabase());
		Connection targetConnection = context.getConnection(context.getTargetDatabase());

		Dialect dialect = context.getDialect();
		ExecutorService executor = context.getExecutor();
		
		boolean sourceAutoCommit = sourceConnection.getAutoCommit();
		boolean targetAutoCommit = targetConnection.getAutoCommit();
		
		targetConnection.setAutoCommit(true);
		
		SynchronizationSupport support = context.getSynchronizationSupport();
		
		support.dropForeignKeys();
		support.dropUniqueConstraints();
		
		sourceConnection.setAutoCommit(false);
		targetConnection.setAutoCommit(false);
		
		try
		{
			try
			{
				for (TableProperties table: context.getSourceDatabaseProperties().getTables())
				{
					String tableName = table.getName();
					
					UniqueConstraint primaryKey = table.getPrimaryKey();
					
					if (primaryKey == null)
					{
						throw new SQLException(Messages.PRIMARY_KEY_REQUIRED.getMessage(this.getClass().getName(), tableName));
					}
					
					List<String> primaryKeyColumns = primaryKey.getColumnList();
					
					Collection<String> columns = table.getColumns();
					
					List<String> nonPrimaryKeyColumns = new ArrayList<String>(columns.size());
					List<String> versionColumns = new ArrayList<String>(columns.size());
					
					for (String column: columns)
					{
						if (!primaryKeyColumns.contains(column))
						{
							// Try to find a version column
							if ((this.versionPattern != null) && this.versionPattern.matcher(column).matches())
							{
								versionColumns.add(column);
							}
							
							nonPrimaryKeyColumns.add(column);
						}
					}
					
					// List of columns for select statement - starting with primary key
					List<String> allColumns = new ArrayList<String>(columns.size());
					allColumns.addAll(primaryKeyColumns);
					allColumns.addAll(nonPrimaryKeyColumns);

					List<String> selectColumns = allColumns;
					if (!versionColumns.isEmpty())
					{
						selectColumns = new ArrayList<String>(primaryKeyColumns.size() + versionColumns.size());
						selectColumns.addAll(primaryKeyColumns);
						selectColumns.addAll(versionColumns);
					}
					
					// Retrieve table rows in primary key order
					final String selectSQL = String.format("SELECT %s FROM %s ORDER BY %s", Strings.join(selectColumns, Strings.PADDED_COMMA), tableName, Strings.join(primaryKeyColumns, Strings.PADDED_COMMA)); //$NON-NLS-1$
					
					final Statement targetStatement = targetConnection.createStatement();
	
					targetStatement.setFetchSize(this.fetchSize);
					
					logger.log(Level.DEBUG, selectSQL);
					
					Callable<ResultSet> callable = new Callable<ResultSet>()
					{
						@Override
						public ResultSet call() throws SQLException
						{
							return targetStatement.executeQuery(selectSQL);
						}
					};
		
					Future<ResultSet> future = executor.submit(callable);
					
					Statement sourceStatement = sourceConnection.createStatement();
					sourceStatement.setFetchSize(this.fetchSize);
					
					ResultSet sourceResultSet = sourceStatement.executeQuery(selectSQL);
	
					ResultSet targetResultSet = future.get();
					
					String primaryKeyWhereClause = Strings.join(new StringBuilder(), primaryKeyColumns, " = ? AND ").append(" = ?").toString(); //$NON-NLS-1$
					
					// Construct DELETE SQL
					String deleteSQL = String.format("DELETE FROM %s WHERE %s", tableName, primaryKeyWhereClause);
					
					logger.log(Level.DEBUG, deleteSQL);
					
					PreparedStatement deleteStatement = targetConnection.prepareStatement(deleteSQL);
					
					PreparedStatement selectAllStatement = null;
					if (!versionColumns.isEmpty())
					{
						String selectAllSQL = String.format("SELECT %s FROM %s WHERE %s", Strings.join(nonPrimaryKeyColumns, Strings.PADDED_COMMA), tableName, primaryKeyWhereClause);
						
						logger.log(Level.DEBUG, selectAllSQL);
						
						selectAllStatement = targetConnection.prepareStatement(selectAllSQL);
					}
					
					// Construct INSERT SQL
					String insertSQL = String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, Strings.join(allColumns, Strings.PADDED_COMMA), Strings.join(Collections.nCopies(allColumns.size(), Strings.QUESTION), Strings.PADDED_COMMA)); //$NON-NLS-1$
					
					logger.log(Level.DEBUG, insertSQL);
					
					PreparedStatement insertStatement = targetConnection.prepareStatement(insertSQL);
					
					// Construct UPDATE SQL
					PreparedStatement updateStatement = null;
					
					if (!nonPrimaryKeyColumns.isEmpty())
					{
						String updateSQL = String.format("UPDATE %s SET %s = ? WHERE %s", tableName, Strings.join(nonPrimaryKeyColumns, " = ?, "), primaryKeyWhereClause);
						
						logger.log(Level.DEBUG, updateSQL);
						
						updateStatement = targetConnection.prepareStatement(updateSQL);
					}
					
					boolean hasMoreSourceResults = sourceResultSet.next();
					boolean hasMoreTargetResults = targetResultSet.next();
					
					int insertCount = 0;
					int updateCount = 0;
					int deleteCount = 0;
					
					while (hasMoreSourceResults || hasMoreTargetResults)
					{
						int compare = 0;
						
						if (!hasMoreSourceResults)
						{
							compare = 1;
						}
						else if (!hasMoreTargetResults)
						{
							compare = -1;
						}
						else
						{
							for (int i = 1; i <= primaryKeyColumns.size(); ++i)
							{
								Object sourceObject = sourceResultSet.getObject(i);
								Object targetObject = targetResultSet.getObject(i);
								
								// We assume that the primary keys column types are Comparable
								compare = this.compare(sourceObject, targetObject);
								
								if (compare != 0)
								{
									break;
								}
							}
						}
						
						if (compare > 0)
						{
							deleteStatement.clearParameters();
							
							for (int i = 1; i <= primaryKeyColumns.size(); ++i)
							{
								int type = dialect.getColumnType(table.getColumnProperties(allColumns.get(i - 1)));
								
								deleteStatement.setObject(i, targetResultSet.getObject(i), type);
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
							
							for (int i = 1; i <= primaryKeyColumns.size(); ++i)
							{
								int type = dialect.getColumnType(table.getColumnProperties(allColumns.get(i - 1)));
								
								insertStatement.setObject(i, sourceResultSet.getObject(i), type);
							}
							
							if (versionColumns.isEmpty())
							{
								for (int i = primaryKeyColumns.size() + 1; i <= allColumns.size(); ++i)
								{
									int type = dialect.getColumnType(table.getColumnProperties(allColumns.get(i - 1)));
	
									Object object = support.getObject(sourceResultSet, i, type);
									
									if (sourceResultSet.wasNull())
									{
										insertStatement.setNull(i, type);
									}
									else
									{
										insertStatement.setObject(i, object, type);
									}
								}
							}
							else
							{
								if (selectAllStatement != null)
								{
									selectAllStatement.clearParameters();
									
									for (int i = 1; i <= primaryKeyColumns.size(); ++i)
									{
										int type = dialect.getColumnType(table.getColumnProperties(allColumns.get(i - 1)));
										
										selectAllStatement.setObject(i, sourceResultSet.getObject(i), type);
									}
									
									ResultSet selectAllResultSet = selectAllStatement.executeQuery();

									for (int i = primaryKeyColumns.size() + 1; i <= allColumns.size(); ++i)
									{
										int type = dialect.getColumnType(table.getColumnProperties(allColumns.get(i - 1)));
										
										Object object = support.getObject(selectAllResultSet, i - primaryKeyColumns.size(), type);
										
										if (selectAllResultSet.wasNull())
										{
											insertStatement.setNull(i, type);
										}
										else
										{
											insertStatement.setObject(i, object, type);
										}
									}
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
						else if (updateStatement != null) // if (compare == 0)
						{
							updateStatement.clearParameters();
							
							boolean updated = false;
							
							for (int i = primaryKeyColumns.size() + 1; i <= selectColumns.size(); ++i)
							{
								int type = dialect.getColumnType(table.getColumnProperties(selectColumns.get(i - 1)));
								
								Object sourceObject = support.getObject(sourceResultSet, i, type);
								Object targetObject = support.getObject(targetResultSet, i, type);
								
								int index = i - primaryKeyColumns.size();
								
								if (sourceResultSet.wasNull())
								{
									updateStatement.setNull(index, type);
									
									updated |= !targetResultSet.wasNull();
								}
								else
								{
									updateStatement.setObject(index, sourceObject, type);
									
									updated |= targetResultSet.wasNull();
									updated |= !Objects.equals(sourceObject, targetObject);
								}
							}
							
							if (updated)
							{
								if (selectAllStatement != null)
								{
									selectAllStatement.clearParameters();
									
									for (int i = 1; i <= primaryKeyColumns.size(); ++i)
									{
										int type = dialect.getColumnType(table.getColumnProperties(allColumns.get(i - 1)));
										
										selectAllStatement.setObject(i, sourceResultSet.getObject(i), type);
									}
									
									ResultSet selectAllResultSet = selectAllStatement.executeQuery();

									for (int i = primaryKeyColumns.size() + 1; i <= allColumns.size(); ++i)
									{
										int type = dialect.getColumnType(table.getColumnProperties(allColumns.get(i - 1)));
										
										int index = i - primaryKeyColumns.size();
										
										Object object = support.getObject(selectAllResultSet, index, type);
										
										if (selectAllResultSet.wasNull())
										{
											updateStatement.setNull(index, type);
										}
										else
										{
											updateStatement.setObject(index, object, type);
										}
									}
								}
								
								for (int i = 1; i <= primaryKeyColumns.size(); ++i)
								{
									int type = dialect.getColumnType(table.getColumnProperties(allColumns.get(i - 1)));
									
									updateStatement.setObject(i + nonPrimaryKeyColumns.size(), targetResultSet.getObject(i), type);
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
						
						if (hasMoreSourceResults && (compare <= 0))
						{
							hasMoreSourceResults = sourceResultSet.next();
						}
						
						if (hasMoreTargetResults && (compare >= 0))
						{
							hasMoreTargetResults = targetResultSet.next();
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
					
					if (updateStatement != null)
					{
						if ((updateCount % this.maxBatchSize) > 0)
						{
							updateStatement.executeBatch();
						}
						
						updateStatement.close();
					}
					
					if (selectAllStatement != null)
					{
						selectAllStatement.close();
					}
					
					targetStatement.close();
					sourceStatement.close();
					
					targetConnection.commit();
					
					logger.log(Level.INFO, Messages.INSERT_COUNT.getMessage(), insertCount, tableName);
					logger.log(Level.INFO, Messages.UPDATE_COUNT.getMessage(), updateCount, tableName);
					logger.log(Level.INFO, Messages.DELETE_COUNT.getMessage(), deleteCount, tableName);
				}
			}
			catch (ExecutionException e)
			{
				support.rollback(targetConnection);
				
				throw ExceptionType.getExceptionFactory(SQLException.class).createException(e.getCause());
			}
			catch (InterruptedException e)
			{
				support.rollback(targetConnection);
				
				throw ExceptionType.getExceptionFactory(SQLException.class).createException(e);
			}
			catch (SQLException e)
			{
				support.rollback(targetConnection);
				
				throw e;
			}
			
			targetConnection.setAutoCommit(true);
			
			support.restoreUniqueConstraints();
			support.restoreForeignKeys();
			
			support.synchronizeIdentityColumns();
			support.synchronizeSequences();
		}
		finally
		{
			sourceConnection.setAutoCommit(sourceAutoCommit);
			targetConnection.setAutoCommit(targetAutoCommit);
		}
	}
	
	private int compare(Object object1, Object object2)
	{
		@SuppressWarnings("unchecked")
		Comparable<Object> comparable = (Comparable<Object>) object1;
		
		return comparable.compareTo(object2);
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

	/**
	 * @return the timestampPattern
	 */
	public String getVersionPattern()
	{
		return this.versionPattern.pattern();
	}

	/**
	 * @param versionPattern the timestampPattern to set
	 */
	public void setVersionPattern(String pattern)
	{
		this.versionPattern = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
	}
}
