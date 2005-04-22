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
package net.sf.hajdbc.sync;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import net.sf.hajdbc.Messages;
import net.sf.hajdbc.SynchronizationStrategy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Database-independent synchronization strategy that only updates differences between two databases.
 * This strategy is best used when there are <em>few</em> differences between the active database and the inactive database (i.e. barely out of sync).
 * The following algorithm is used:
 * <ol>
 *  <li>Drop the foreign keys on the inactive database (to avoid integrity constraint violations)</li>
 *  <li>For each database table:
 *   <ol>
 *    <li>Find the primary key(s) of the table</li>
 *    <li>Query all rows in the inactive database table, sorting by the primary key(s)</li>
 *    <li>Query all rows on the active database table</li>
 *    <li>For each row in table:
 *     <ol>
 *      <li>If primary key of the rows are the same, determine whether or not row needs to be updated</li>
 *      <li>Otherwise, determine whether row should be deleted, or a new row is to be inserted</li>
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
public class DifferentialSynchronizationStrategy implements SynchronizationStrategy
{
	private static Log log = LogFactory.getLog(DifferentialSynchronizationStrategy.class);

	private String createForeignKeySQL = ForeignKey.DEFAULT_CREATE_SQL;
	private String dropForeignKeySQL = ForeignKey.DEFAULT_DROP_SQL;
	private int fetchSize = 0;
	
	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#synchronize(java.sql.Connection, java.sql.Connection, java.util.List)
	 */
	public void synchronize(Connection inactiveConnection, Connection activeConnection, List tableList) throws SQLException
	{
		inactiveConnection.setAutoCommit(true);
		
		// Drop foreign keys
		ForeignKey.executeSQL(inactiveConnection, ForeignKey.collect(inactiveConnection, tableList), this.dropForeignKeySQL);
		
		inactiveConnection.setAutoCommit(false);
		
		DatabaseMetaData databaseMetaData = inactiveConnection.getMetaData();
		
		List primaryKeyList = new ArrayList();
		Set primaryKeyColumnSet = new LinkedHashSet();
		
		Iterator tables = tableList.iterator();
		
		while (tables.hasNext())
		{	
			String table = (String) tables.next();
			
			primaryKeyList.clear();
			primaryKeyColumnSet.clear();
			
			// Fetch primary keys of this table
			ResultSet primaryKeyResultSet = databaseMetaData.getPrimaryKeys(null, null, table);
			
			while (primaryKeyResultSet.next())
			{
				primaryKeyList.add(primaryKeyResultSet.getString("COLUMN_NAME"));
			}
			
			primaryKeyResultSet.close();
			
			if (primaryKeyList.isEmpty())
			{
				throw new SQLException(Messages.getMessage(Messages.PRIMARY_KEY_REQUIRED, new Object[] { this.getClass().getName(), table }));
			}

			// Fetch row count of table from inactive connection
			Statement statement = inactiveConnection.createStatement();
			ResultSet resultSet = statement.executeQuery("SELECT count(*) FROM " + table);
			resultSet.next();
			int rows = resultSet.getInt(1);
			statement.close();
			
			// Retrieve table rows in primary key order
			StringBuffer buffer = new StringBuffer("SELECT * FROM ").append(table).append(" ORDER BY ");
			
			Iterator primaryKeys = primaryKeyList.iterator();
			
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
			inactiveStatement.setFetchSize(this.fetchSize);

			Statement activeStatement = activeConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			activeStatement.setFetchSize(this.fetchSize);
			
			if (log.isDebugEnabled())
			{
				log.debug(sql);
			}
			
			Thread statementExecutor = new Thread(new StatementExecutor(inactiveStatement, sql));
			statementExecutor.start();

			ResultSet activeResultSet = activeStatement.executeQuery(sql);
			
			try
			{
				statementExecutor.join();
			}
			catch (InterruptedException e)
			{
				// Statement executor cannot be interrupted
			}
			
			ResultSet inactiveResultSet = inactiveStatement.getResultSet();

			// If query failed, result set will be null
			if (inactiveResultSet == null)
			{
				// Fetch exception
				throw inactiveStatement.getWarnings();
			}
			
			// Create set of primary key columns
			primaryKeys = primaryKeyList.iterator();
			
			while (primaryKeys.hasNext())
			{
				String primaryKey = (String) primaryKeys.next();
				primaryKeyColumnSet.add(new Integer(activeResultSet.findColumn(primaryKey)));
			}
			
			boolean hasMoreActiveResults = activeResultSet.next();
			boolean hasMoreInactiveResults = inactiveResultSet.next();

			ResultSetMetaData resultSetMetaData = activeResultSet.getMetaData();

			int columns = resultSetMetaData.getColumnCount();
			
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
				
				if (hasMoreActiveResults && (compare <= 0))
				{
					hasMoreActiveResults = activeResultSet.next();
				}
				
				if (hasMoreInactiveResults && (compare >= 0))
				{
					// The ResultSet may have been affected by calls to insertRow(), so use pre-determined row count as additional criteria to determine if there are more results
					hasMoreInactiveResults = inactiveResultSet.next() && (inactiveResultSet.getRow() <= rows);
				}
			}
			
			log.info(Messages.getMessage(Messages.INSERT_COUNT, new Object[] { new Integer(insertCount), table }));
			log.info(Messages.getMessage(Messages.UPDATE_COUNT, new Object[] { new Integer(updateCount), table }));
			log.info(Messages.getMessage(Messages.DELETE_COUNT, new Object[] { new Integer(deleteCount), table }));
			
			inactiveStatement.close();
			activeStatement.close();
			
			inactiveConnection.commit();
		}

		inactiveConnection.setAutoCommit(true);

		// Recreate foreign keys
		ForeignKey.executeSQL(inactiveConnection, ForeignKey.collect(activeConnection, tableList), this.createForeignKeySQL);
	}
	
	/**
	 * @return the createForeignKeySQL.
	 */
	public String getCreateForeignKeySQL()
	{
		return this.createForeignKeySQL;
	}
	
	/**
	 * @param createForeignKeySQL the createForeignKeySQL to set.
	 */
	public void setCreateForeignKeySQL(String createForeignKeySQL)
	{
		this.createForeignKeySQL = createForeignKeySQL;
	}
	
	/**
	 * @return the dropForeignKeySQL.
	 */
	public String getDropForeignKeySQL()
	{
		return this.dropForeignKeySQL;
	}
	
	/**
	 * @param dropForeignKeySQL the dropForeignKeySQL to set.
	 */
	public void setDropForeignKeySQL(String dropForeignKeySQL)
	{
		this.dropForeignKeySQL = dropForeignKeySQL;
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
