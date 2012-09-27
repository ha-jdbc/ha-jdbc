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
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.ExceptionType;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.TableProperties;
import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;
import net.sf.hajdbc.util.Resources;
import net.sf.hajdbc.util.Strings;

/**
 * Database-independent synchronization strategy that does full record transfer between two databases.
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
 */
public class FullSynchronizationStrategy implements SynchronizationStrategy, TableSynchronizationStrategy, Serializable
{
	private static final long serialVersionUID = 9190347092842178162L;

	private static Logger logger = LoggerFactory.getLogger(FullSynchronizationStrategy.class);

	private SynchronizationStrategy strategy = new PerTableSynchronizationStrategy(this);
	private int maxBatchSize = 100;
	private int fetchSize = 0;

	@Override
	public String getId()
	{
		return "full";
	}

	@Override
	public <Z, D extends Database<Z>> void init(DatabaseCluster<Z, D> cluster)
	{
		this.strategy.init(cluster);
	}

	@Override
	public <Z, D extends Database<Z>> void synchronize(SynchronizationContext<Z, D> context) throws SQLException
	{
		this.strategy.synchronize(context);
	}

	@Override
	public <Z, D extends Database<Z>> void destroy(DatabaseCluster<Z, D> cluster)
	{
		this.strategy.destroy(cluster);
	}

	@Override
	public <Z, D extends Database<Z>> void synchronize(SynchronizationContext<Z, D> context, TableProperties table) throws SQLException
	{
		final String tableName = table.getName().getDMLName();
		final Collection<String> columns = table.getColumns();
		
		final String commaDelimitedColumns = Strings.join(columns, Strings.PADDED_COMMA);
		
		final String selectSQL = String.format("SELECT %s FROM %s", commaDelimitedColumns, tableName);
		final String deleteSQL = context.getDialect().getTruncateTableSQL(table);
		final String insertSQL = String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, commaDelimitedColumns, Strings.join(Collections.nCopies(columns.size(), Strings.QUESTION), Strings.PADDED_COMMA));
		
		Connection sourceConnection = context.getConnection(context.getSourceDatabase());
		final Statement selectStatement = sourceConnection.createStatement();
		try
		{
			selectStatement.setFetchSize(this.fetchSize);
			
			Callable<ResultSet> callable = new Callable<ResultSet>()
			{
				@Override
				public ResultSet call() throws SQLException
				{
					logger.log(Level.DEBUG, selectSQL);
					return selectStatement.executeQuery(selectSQL);
				}
			};
	
			Future<ResultSet> future = context.getExecutor().submit(callable);
			
			Connection targetConnection = context.getConnection(context.getTargetDatabase());
			Statement deleteStatement = targetConnection.createStatement();
			
			try
			{
				logger.log(Level.DEBUG, deleteSQL);
				int deletedRows = deleteStatement.executeUpdate(deleteSQL);
		
				logger.log(Level.INFO, Messages.DELETE_COUNT.getMessage(), deletedRows, tableName);
			}
			finally
			{
				Resources.close(deleteStatement);
			}
			
			try
			{
				ResultSet resultSet = future.get();
				
				logger.log(Level.DEBUG, insertSQL);
				PreparedStatement insertStatement = targetConnection.prepareStatement(insertSQL);
				try
				{
					int statementCount = 0;
					
					while (resultSet.next())
					{
						int index = 0;
						
						for (String column: table.getColumns())
						{
							index += 1;
							
							int type = context.getDialect().getColumnType(table.getColumnProperties(column));
							
							Object object = context.getSynchronizationSupport().getObject(resultSet, index, type);
							
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
			
					logger.log(Level.INFO, Messages.INSERT_COUNT.getMessage(), statementCount, table);
				}
				finally
				{
					Resources.close(insertStatement);
				}
			}
			catch (ExecutionException e)
			{
				throw ExceptionType.getExceptionFactory(SQLException.class).createException(e.getCause());
			}
			catch (InterruptedException e)
			{
				Thread.currentThread().interrupt();
				throw new SQLException(e);
			}
		}
		finally
		{
			Resources.close(selectStatement);
		}
	}
	
	@Override
	public <Z, D extends Database<Z>> void dropConstraints(SynchronizationContext<Z, D> context) throws SQLException
	{
		context.getSynchronizationSupport().dropForeignKeys();
	}

	@Override
	public <Z, D extends Database<Z>> void restoreConstraints(SynchronizationContext<Z, D> context) throws SQLException
	{
		context.getSynchronizationSupport().restoreForeignKeys();
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
