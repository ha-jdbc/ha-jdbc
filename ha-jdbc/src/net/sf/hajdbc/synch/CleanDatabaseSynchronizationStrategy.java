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
package net.sf.hajdbc.synch;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import net.sf.hajdbc.ConnectionProxy;
import net.sf.hajdbc.DatabaseClusterDescriptor;
import net.sf.hajdbc.DatabaseSynchronizationStrategy;
import net.sf.hajdbc.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class CleanDatabaseSynchronizationStrategy implements DatabaseSynchronizationStrategy
{
	private static final int MAX_BATCH_SIZE = 50;
	
	private static Log log = LogFactory.getLog(CleanDatabaseSynchronizationStrategy.class);

	/**
	 * @see net.sf.hajdbc.DatabaseSynchronizationStrategy#synchronize(java.sql.Connection, net.sf.hajdbc.ConnectionProxy, java.util.List)
	 */
	public void synchronize(Connection connection, ConnectionProxy connectionProxy, List tableList) throws java.sql.SQLException
	{
		DatabaseClusterDescriptor descriptor = connectionProxy.getDatabaseCluster().getDescriptor();
		
		String createForeignKeySQL = descriptor.getCreateForeignKeySQL();
		String createIndexSQL = descriptor.getCreateIndexSQL();
		String dropForeignKeySQL = descriptor.getDropForeignKeySQL();
		String dropIndexSQL = descriptor.getDropIndexSQL();
		
		connection.setAutoCommit(false);

		PreparedStatement pingStatement = connectionProxy.prepareStatement(descriptor.getValidateSQL());
		
		Iterator tables = tableList.iterator();
		
		while (tables.hasNext())
		{
			String table = (String) tables.next();
			
			Collection foreignKeyCollection = ForeignKey.getForeignKeys(connection, table);
			Collection indexCollection = Index.getIndexes(connection, table);
			
			ForeignKey.executeSQL(connection, foreignKeyCollection, dropForeignKeySQL);
			
			connection.commit();
			
			Index.executeSQL(connection, indexCollection, dropIndexSQL);
			
			connection.commit();
			
			String deleteSQL = "DELETE FROM " + table;
			String selectSQL = "SELECT * FROM " + table;

			log.info("Deleting: " + deleteSQL);
			
			Statement deleteStatement = connection.createStatement();
			Statement selectStatement = connectionProxy.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			
			Thread deleteExecutor = new Thread(new StatementExecutor(deleteStatement, deleteSQL));
			deleteExecutor.start();

			ResultSet resultSet = selectStatement.executeQuery(selectSQL);
			
			try
			{
				deleteExecutor.join();
			}
			catch (InterruptedException e)
			{
				throw new SQLException("Execution of " + deleteSQL + " was interrupted.", e);
			}
			
			int deletedRows = deleteStatement.getUpdateCount();
			
			if (deletedRows < 0)
			{
				throw deleteStatement.getWarnings();
			}
			
			log.info("Deleted " + deletedRows + " rows from " + table);
			
			deleteStatement.close();
			
			StringBuffer insertSQL = new StringBuffer("INSERT INTO ").append(table).append(" VALUES (");

			ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
			
			int columns = resultSetMetaData.getColumnCount();
/*
			for (int i = 1; i <= columns; ++i)
			{
				if (i > 1)
				{
					insertSQL.append(", ");
				}
				
				insertSQL.append(resultSetMetaData.getColumnName(i));
			}
			
			insertSQL.append(") VALUES (");
*/			
			for (int i = 1; i <= columns; ++i)
			{
				if (i > 1)
				{
					insertSQL.append(", ");
				}
				
				insertSQL.append("?");
			}
			
			insertSQL.append(")");
			
			log.info("Inserting: " + insertSQL);
			
			PreparedStatement insertStatement = connection.prepareStatement(insertSQL.toString());
			int statementCount = 0;
			
			while (resultSet.next())
			{
				insertStatement.clearParameters();
				
				for (int i = 1; i <= columns; ++i)
				{
					Object object = resultSet.getObject(i);
					
					if (resultSet.wasNull())
					{
						insertStatement.setNull(i, resultSetMetaData.getColumnType(i));
					}
					else
					{
						insertStatement.setObject(i, object);
					}
				}
				
				insertStatement.addBatch();
				statementCount += 1;
				
				if ((statementCount % MAX_BATCH_SIZE) == 0)
				{
					insertStatement.executeBatch();
				}
			}

			if (statementCount > 0)
			{
				insertStatement.executeBatch();
			}

			log.info("Inserted " + statementCount + " rows into " + table);
			
			insertStatement.close();
			selectStatement.close();
			
			connection.commit();
			
			Index.executeSQL(connection, indexCollection, createIndexSQL);
			
			connection.commit();
			
			ForeignKey.executeSQL(connection, foreignKeyCollection, createForeignKeySQL);
			
			connection.commit();
			
			// Ping databases so they don't timeout
			pingStatement.execute();
		}
		
		pingStatement.close();
	}
}
