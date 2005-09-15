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

import net.sf.hajdbc.Messages;

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
public class DifferentialSynchronizationStrategy extends AbstractSynchronizationStrategy
{
	private static Log log = LogFactory.getLog(DifferentialSynchronizationStrategy.class);

	private String createUniqueKeySQL = UniqueKey.DEFAULT_CREATE_SQL;
	private String dropUniqueKeySQL = UniqueKey.DEFAULT_DROP_SQL;
	
	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#synchronize(java.sql.Connection, java.sql.Connection, java.util.List)
	 */
	public void synchronize(Connection inactiveConnection, Connection activeConnection, List tableList) throws SQLException
	{
		inactiveConnection.setAutoCommit(true);
		
		// Drop foreign keys
		Key.executeSQL(inactiveConnection, ForeignKey.collect(inactiveConnection, tableList), this.dropForeignKeySQL);
		
		inactiveConnection.setAutoCommit(false);
		
		DatabaseMetaData databaseMetaData = inactiveConnection.getMetaData();
		String quote = databaseMetaData.getIdentifierQuoteString();
		
		Map primaryKeyColumnMap = new TreeMap();
		Set primaryKeyColumnIndexSet = new LinkedHashSet();
		
		Iterator tables = tableList.iterator();
		
		while (tables.hasNext())
		{	
			String table = (String) tables.next();
			
			primaryKeyColumnMap.clear();
			primaryKeyColumnIndexSet.clear();
			
			// Fetch primary keys of this table
			ResultSet primaryKeyResultSet = databaseMetaData.getPrimaryKeys(null, null, table);
			String primaryKeyName = null;
			
			while (primaryKeyResultSet.next())
			{
				String name = primaryKeyResultSet.getString("COLUMN_NAME");
				short position = primaryKeyResultSet.getShort("KEY_SEQ");

				primaryKeyColumnMap.put(new Short(position), name);
				
				primaryKeyName = primaryKeyResultSet.getString("PK_NAME");
			}
			
			primaryKeyResultSet.close();
			
			if (primaryKeyColumnMap.isEmpty())
			{
				throw new SQLException(Messages.getMessage(Messages.PRIMARY_KEY_REQUIRED, new Object[] { this.getClass().getName(), table }));
			}

			Key.executeSQL(inactiveConnection, UniqueKey.collect(inactiveConnection, table, primaryKeyName), this.dropUniqueKeySQL);
			
			// Retrieve table rows in primary key order
			StringBuffer buffer = new StringBuffer("SELECT * FROM ").append(quote).append(table).append(quote).append(" ORDER BY ");
			
			Iterator primaryKeyColumns = primaryKeyColumnMap.values().iterator();
			
			while (primaryKeyColumns.hasNext())
			{
				buffer.append(quote).append(primaryKeyColumns.next()).append(quote);
				
				if (primaryKeyColumns.hasNext())
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
			
			// Construct DELETE SQL
			StringBuffer deleteSQL = new StringBuffer("DELETE FROM ").append(quote).append(table).append(quote).append(" WHERE ");
			
			// Create set of primary key columns
			primaryKeyColumns = primaryKeyColumnMap.values().iterator();
			
			while (primaryKeyColumns.hasNext())
			{
				String primaryKeyColumn = (String) primaryKeyColumns.next();
				
				primaryKeyColumnIndexSet.add(new Integer(activeResultSet.findColumn(primaryKeyColumn)));
				
				if (primaryKeyColumns.hasNext())
				{
					deleteSQL.append(" AND ");
				}
				
				deleteSQL.append(quote).append(primaryKeyColumn).append(quote).append(" = ?");
			}

			PreparedStatement deleteStatement = inactiveConnection.prepareStatement(deleteSQL.toString());
			
			ResultSetMetaData resultSetMetaData = activeResultSet.getMetaData();
			int columns = resultSetMetaData.getColumnCount();
			int[] types = new int[columns + 1];
			
			// Construct INSERT SQL
			StringBuffer insertSQL = new StringBuffer("INSERT INTO ").append(quote).append(table).append(quote).append(" (");
			
			for (int i = 1; i <= columns; ++i)
			{
				types[i] = resultSetMetaData.getColumnType(i);
				
				if (i > 1)
				{
					insertSQL.append(", ");
				}
				
				insertSQL.append(quote).append(resultSetMetaData.getColumnName(i)).append(quote);
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
					Iterator primaryKeyColumnIndexes = primaryKeyColumnIndexSet.iterator();
					
					while (primaryKeyColumnIndexes.hasNext())
					{
						Integer primaryKeyColumnIndex = (Integer) primaryKeyColumnIndexes.next();
						int column = primaryKeyColumnIndex.intValue();
						
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
					
					Iterator primaryKeyColumnIndexes = primaryKeyColumnIndexSet.iterator();
					int index = 1;
					
					while (primaryKeyColumnIndexes.hasNext())
					{
						Integer primaryKeyColumnIndex = (Integer) primaryKeyColumnIndexes.next();
						int column = primaryKeyColumnIndex.intValue();
						
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
						if (!primaryKeyColumnIndexSet.contains(new Integer(i)))
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

			Key.executeSQL(inactiveConnection, UniqueKey.collect(activeConnection, table, primaryKeyName), this.createUniqueKeySQL);
			
			inactiveConnection.commit();
			
			log.info(Messages.getMessage(Messages.INSERT_COUNT, new Object[] { new Integer(insertCount), table }));
			log.info(Messages.getMessage(Messages.UPDATE_COUNT, new Object[] { new Integer(updateCount), table }));
			log.info(Messages.getMessage(Messages.DELETE_COUNT, new Object[] { new Integer(deleteCount), table }));			
		}

		inactiveConnection.setAutoCommit(true);

		// Recreate foreign keys
		Key.executeSQL(inactiveConnection, ForeignKey.collect(activeConnection, tableList), this.createForeignKeySQL);
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
	 * @return the createUniqueKeySQL.
	 */
	public String getCreateUniqueKeySQL()
	{
		return this.createUniqueKeySQL;
	}

	/**
	 * @param createUniqueKeySQL the createUniqueKeySQL to set.
	 */
	public void setCreateUniqueKeySQL(String createUniqueKeySQL)
	{
		this.createUniqueKeySQL = createUniqueKeySQL;
	}

	/**
	 * @return the dropUniqueKeySQL.
	 */
	public String getDropUniqueKeySQL()
	{
		return this.dropUniqueKeySQL;
	}

	/**
	 * @param dropUniqueKeySQL the dropUniqueKeySQL to set.
	 */
	public void setDropUniqueKeySQL(String dropUniqueKeySQL)
	{
		this.dropUniqueKeySQL = dropUniqueKeySQL;
	}
}
