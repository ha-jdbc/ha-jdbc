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
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.SynchronizationContext;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.TableProperties;
import net.sf.hajdbc.util.Strings;
import net.sf.hajdbc.util.concurrent.DaemonThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 *  <li>Synchronize sequences</li>
 * </ol>
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class FullSynchronizationStrategy implements SynchronizationStrategy
{
	private static Logger logger = LoggerFactory.getLogger(FullSynchronizationStrategy.class);

	private ExecutorService executor = Executors.newSingleThreadExecutor(DaemonThreadFactory.getInstance());
	private int maxBatchSize = 100;
	private int fetchSize = 0;
	private SynchronizationSupport support = new SynchronizationSupport();
	
	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#cleanup(net.sf.hajdbc.SynchronizationContext)
	 */
	public void cleanup(SynchronizationContext context)
	{
		this.support.unlock(context);
	}

	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#prepare(net.sf.hajdbc.SynchronizationContext)
	 */
	public void prepare(SynchronizationContext context) throws SQLException
	{
		this.support.lock(context);
	}

	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#synchronize(net.sf.hajdbc.SynchronizationContext)
	 */
	public void synchronize(SynchronizationContext context) throws SQLException
	{
		Connection sourceConnection = context.getConnection(context.getSourceDatabase());
		Connection targetConnection = context.getConnection(context.getTargetDatabase());

		Dialect dialect = context.getDialect();
		
		targetConnection.setAutoCommit(true);
		
		this.support.dropForeignKeys(context);
		
		targetConnection.setAutoCommit(false);
		
		try
		{
			for (TableProperties table: context.getDatabaseMetaDataCache().getDatabaseProperties(sourceConnection).getTables())
			{
				String tableName = table.getName();
				Collection<String> columns = table.getColumns();
				
				String commaDelimitedColumns = Strings.join(columns, ", ");
				
				final String selectSQL = "SELECT " + commaDelimitedColumns + " FROM " + tableName;
				
				final Statement selectStatement = sourceConnection.createStatement();
				selectStatement.setFetchSize(this.fetchSize);
				
				Callable<ResultSet> callable = new Callable<ResultSet>()
				{
					public ResultSet call() throws SQLException
					{
						return selectStatement.executeQuery(selectSQL);
					}
				};
	
				Future<ResultSet> future = this.executor.submit(callable);
				
				String deleteSQL = dialect.getTruncateTableSQL(table);
	
				logger.debug(deleteSQL);
				
				Statement deleteStatement = targetConnection.createStatement();
	
				int deletedRows = deleteStatement.executeUpdate(deleteSQL);
				
				logger.info(Messages.getMessage(Messages.DELETE_COUNT, deletedRows, tableName));
				
				deleteStatement.close();
				
				ResultSet resultSet = future.get();
				
				String[] parameters = new String[columns.size()];
				Arrays.fill(parameters, "?");
				
				String insertSQL = "INSERT INTO " + tableName + " (" + commaDelimitedColumns + ") VALUES (" + Strings.join(Arrays.asList(parameters), ", ") + ")";
				
				logger.debug(insertSQL);
				
				PreparedStatement insertStatement = targetConnection.prepareStatement(insertSQL);
				int statementCount = 0;
				
				while (resultSet.next())
				{
					int index = 0;
					
					for (String column: columns)
					{
						index += 1;
						
						int type = dialect.getColumnType(table.getColumnProperties(column));
						
						Object object = this.support.getObject(resultSet, index, type);
						
						if (resultSet.wasNull())
						{
							insertStatement.setNull(index, type);
						}
						else
						{
							insertStatement.setObject(index, object, type);
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
	
				logger.info(Messages.getMessage(Messages.INSERT_COUNT, statementCount, tableName));
				
				insertStatement.close();
				selectStatement.close();
				
				targetConnection.commit();
			}
		}
		catch (InterruptedException e)
		{
			this.support.rollback(targetConnection);

			throw new net.sf.hajdbc.SQLException(e);
		}
		catch (ExecutionException e)
		{
			this.support.rollback(targetConnection);

			throw new net.sf.hajdbc.SQLException(e.getCause());
		}
		catch (SQLException e)
		{
			this.support.rollback(targetConnection);
			
			throw e;
		}
		
		targetConnection.setAutoCommit(true);
		
		this.support.restoreForeignKeys(context);
		
		this.support.synchronizeSequences(context);
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
