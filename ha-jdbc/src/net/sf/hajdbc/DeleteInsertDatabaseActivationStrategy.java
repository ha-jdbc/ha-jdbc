/*
 * Copyright (c) 2004, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class DeleteInsertDatabaseActivationStrategy implements DatabaseActivationStrategy
{
	private static final int MAX_BATCH_SIZE = 50;
	
	private static Log log = LogFactory.getLog(DeleteInsertDatabaseActivationStrategy.class);

	/**
	 * @see net.sf.hajdbc.DatabaseActivationStrategy#activate(net.sf.hajdbc.DatabaseCluster, net.sf.hajdbc.Database)
	 */
	public void activate(DatabaseCluster databaseCluster, Database database) throws SQLException
	{
		Connection activeConnection = null;
		Connection inactiveConnection = null;
		
		List foreignKeyList = new LinkedList();
		
		try
		{
			Database activeDatabase = databaseCluster.getDescriptor().firstDatabase();
			
			inactiveConnection = database.connect(databaseCluster.getSQLObject(database));
			inactiveConnection.setAutoCommit(false);
			
			DatabaseMetaData databaseMetaData = inactiveConnection.getMetaData();
			List tableList = new LinkedList();
			
			ResultSet resultSet = databaseMetaData.getTables(null, null, "%", new String[] { "TABLE" });
			
			while (resultSet.next())
			{
				String table = resultSet.getString("TABLE_NAME");
				tableList.add(table);
			}
			
			resultSet.close();

			Iterator tables = tableList.iterator();
			
			while (tables.hasNext())
			{
				String table = (String) tables.next();
				
				resultSet = databaseMetaData.getImportedKeys(null, null, table);
				
				while (resultSet.next())
				{
					String name = resultSet.getString("FK_NAME");
					String column = resultSet.getString("FKCOLUMN_NAME");
					String foreignTable = resultSet.getString("PKTABLE_NAME");
					String foreignColumn = resultSet.getString("PKCOLUMN_NAME");
					
					ForeignKey foreignKey = new ForeignKey(name, table, column, foreignTable, foreignColumn);
					String sql = foreignKey.dropSQL();
					
					log.info("Dropping foreign key: " + sql);
					
					Statement statement = inactiveConnection.createStatement();
					statement.execute(foreignKey.dropSQL());
					statement.close();
					
					foreignKeyList.add(foreignKey);
				}
				
				resultSet.close();
			}
			
			inactiveConnection.commit();
			
			activeConnection = activeDatabase.connect(databaseCluster.getSQLObject(activeDatabase));
			activeConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			activeConnection.setAutoCommit(false);
			
			tables = tableList.iterator();
			
			while (tables.hasNext())
			{
				String table = (String) tables.next();

				String deleteSQL = "DELETE FROM " + table;

				log.info("Deleting: " + deleteSQL);
				
				Statement deleteStatement = inactiveConnection.createStatement();
				deleteStatement.executeUpdate(deleteSQL);
				deleteStatement.close();
				
				Statement selectStatement = activeConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				
				resultSet = selectStatement.executeQuery("SELECT * FROM " + table);
				ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
				
				int columns = resultSetMetaData.getColumnCount();

				StringBuffer insertSQL = new StringBuffer("INSERT INTO ").append(table).append(" (");

				for (int i = 1; i <= columns; ++i)
				{
					if (i > 1)
					{
						insertSQL.append(", ");
					}
					
					insertSQL.append(resultSetMetaData.getColumnName(i));
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
				
				log.info("Inserting: " + insertSQL);
				PreparedStatement insertStatement = inactiveConnection.prepareStatement(insertSQL.toString());
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
					
					if (statementCount >= MAX_BATCH_SIZE)
					{
						insertStatement.executeBatch();
						statementCount = 0;
					}
				}

				if (statementCount > 0)
				{
					insertStatement.executeBatch();
				}
				
				insertStatement.close();
				selectStatement.close();
				
				inactiveConnection.commit();
			}

			log.info("Database synchronization completed successfully");
		}
		finally
		{
			if ((inactiveConnection != null) && !inactiveConnection.isClosed())
			{
				try
				{
					inactiveConnection.rollback();
					
					Iterator foreignKeys = foreignKeyList.iterator();
					
					while (foreignKeys.hasNext())
					{
						ForeignKey foreignKey = (ForeignKey) foreignKeys.next();
						String sql = foreignKey.dropSQL();
						
						log.info("Recreating foreign key: " + sql);
						
						Statement statement = inactiveConnection.createStatement();
						statement.execute(foreignKey.createSQL());
						statement.close();
					}

					inactiveConnection.commit();
					inactiveConnection.close();
				}
				catch (SQLException e)
				{
					log.warn("Failed to close connection of inactive database", e);
				}
			}
			
			if ((activeConnection != null) && !activeConnection.isClosed())
			{
				try
				{
					activeConnection.rollback();
					activeConnection.close();
				}
				catch (SQLException sqle)
				{
					log.warn("Failed to close connection of active database", sqle);
				}
			}			
		}
	}
}
