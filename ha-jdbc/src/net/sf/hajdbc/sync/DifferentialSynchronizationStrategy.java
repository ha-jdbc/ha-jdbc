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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import net.sf.hajdbc.DatabaseClusterDescriptor;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class DifferentialSynchronizationStrategy implements SynchronizationStrategy
{
	private static Log log = LogFactory.getLog(DifferentialSynchronizationStrategy.class);

	/**
	 * @see net.sf.hajdbc.DatabaseSynchronizationStrategy#synchronize(net.sf.hajdbc.DatabaseClusterDescriptor, java.sql.Connection, java.sql.Connection, java.util.List)
	 */
	public void synchronize(Connection inactiveConnection, Connection activeConnection, List tableList, DatabaseClusterDescriptor descriptor) throws java.sql.SQLException
	{
		inactiveConnection.setAutoCommit(true);
		
		// Drop foreign keys
		ForeignKey.drop(inactiveConnection, ForeignKey.collectForeignKeys(inactiveConnection, tableList), descriptor);
		
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
			Statement activeStatement = activeConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			
			log.info("Updating: " + sql);
			
			Thread statementExecutor = new Thread(new StatementExecutor(inactiveStatement, sql));
			statementExecutor.start();

			ResultSet activeResultSet = activeStatement.executeQuery(sql);
			
			try
			{
				statementExecutor.join();
			}
			catch (InterruptedException e)
			{
				throw new SQLException("Execution of " + sql + " on inactive database was interrupted.", e);
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
					hasMoreInactiveResults = inactiveResultSet.next();
				}
			}
			
			log.info("Inserted " + insertCount + " rows into " + table);
			log.info("Deleted " + deleteCount + " rows from " + table);
			log.info("Updated " + updateCount + " rows in " + table);
			
			inactiveStatement.close();
			activeStatement.close();
			
			inactiveConnection.commit();
		}

		inactiveConnection.setAutoCommit(true);

		// Recreate foreign keys
		ForeignKey.create(inactiveConnection, ForeignKey.collectForeignKeys(activeConnection, tableList), descriptor);
	}
}
