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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.SynchronizationContext;
import net.sf.hajdbc.TableProperties;
import net.sf.hajdbc.UniqueConstraint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Paul Ferraro
 *
 */
public class SynchronizationSupport
{
	private static Logger logger = LoggerFactory.getLogger(SynchronizationSupport.class);
	
	public void dropForeignKeys(SynchronizationContext context) throws SQLException
	{
		Connection connection = context.getConnection(context.getTargetDatabase());
		
		Collection<TableProperties> tables = context.getDatabaseMetaDataCache().getDatabaseProperties(connection).getTables();
		
		Dialect dialect = context.getDialect();
		
		Statement statement = connection.createStatement();
		
		// Drop foreign key constraints on the inactive database
		for (TableProperties table: tables)
		{
			for (ForeignKeyConstraint constraint: table.getForeignKeyConstraints())
			{
				String sql = dialect.getDropForeignKeyConstraintSQL(constraint);
				
				logger.debug(sql);
				
				statement.addBatch(sql);
			}
		}
		
		statement.executeBatch();
		statement.close();
	}
	
	public void restoreForeignKeys(SynchronizationContext context) throws SQLException
	{
		Connection connection = context.getConnection(context.getTargetDatabase());
		
		Collection<TableProperties> tables = context.getDatabaseMetaDataCache().getDatabaseProperties(connection).getTables();
		
		Dialect dialect = context.getDialect();
		
		Statement statement = connection.createStatement();
		
		// Drop foreign key constraints on the inactive database
		for (TableProperties table: tables)
		{
			for (ForeignKeyConstraint constraint: table.getForeignKeyConstraints())
			{
				String sql = dialect.getCreateForeignKeyConstraintSQL(constraint);
				
				logger.debug(sql);
				
				statement.addBatch(sql);
			}
		}
		
		statement.executeBatch();
		statement.close();
	}
	
	public void synchronizeSequences(final SynchronizationContext context) throws SQLException
	{
		Database sourceDatabase = context.getSourceDatabase();		
		Connection sourceConnection = context.getConnection(sourceDatabase);
		
		Dialect dialect = context.getDialect();
		
		Map<String, Long> sequenceMap = new HashMap<String, Long>();

		Collection<String> sequences = dialect.getSequences(sourceConnection);
		Collection<Database> databases = context.getActiveDatabases();

		Map<Database, Future<Long>> futureMap = new HashMap<Database, Future<Long>>();
		ExecutorService executor = Executors.newFixedThreadPool(databases.size());
		
		for (String sequence: sequences)
		{
			final String sql = dialect.getNextSequenceValueSQL(sequence);
			
			logger.debug(sql);

			for (final Database database: databases)
			{
				Callable<Long> task = new Callable<Long>()
				{
					public Long call() throws SQLException
					{
						Statement statement = context.getConnection(database).createStatement();
						ResultSet resultSet = statement.executeQuery(sql);
						
						resultSet.next();
						
						long value = resultSet.getLong(1);
						
						resultSet.close();
						statement.close();
						
						return value;
					}
				};
				
				futureMap.put(database, executor.submit(task));				
			}

			try
			{
				Long sourceValue = futureMap.get(sourceDatabase).get();
				
				sequenceMap.put(sequence, sourceValue);
				
				for (Database database: databases)
				{
					if (!database.equals(sourceDatabase))
					{
						Long value = futureMap.get(database).get();
						
						if (!value.equals(sourceValue))
						{
							throw new SQLException(Messages.getMessage(Messages.SEQUENCE_OUT_OF_SYNC, sequence, database, value, sourceDatabase, sourceValue));
						}
					}
				}
			}
			catch (InterruptedException e)
			{
				throw new net.sf.hajdbc.SQLException(e);
			}
			catch (ExecutionException e)
			{
				throw new net.sf.hajdbc.SQLException(e);
			}
		}
		
		executor.shutdown();
		
		Connection targetConnection = context.getConnection(context.getTargetDatabase());
		Statement targetStatement = targetConnection.createStatement();

		for (String sequence: sequences)
		{
			String sql = dialect.getAlterSequenceSQL(sequence, sequenceMap.get(sequence) + 1);
			
			logger.debug(sql);
			
			targetStatement.addBatch(sql);
		}
		
		targetStatement.executeBatch();		
		targetStatement.close();
	}
	
	public void lock(SynchronizationContext context) throws SQLException
	{
		logger.info(Messages.getMessage(Messages.TABLE_LOCK_ACQUIRE));
		
		Map<TableProperties, String> lockTableSQLMap = new HashMap<TableProperties, String>();
		
		Connection targetConnection = context.getConnection(context.getTargetDatabase());
		
		Collection<TableProperties> tables = context.getDatabaseMetaDataCache().getDatabaseProperties(targetConnection).getTables();
		
		Dialect dialect = context.getDialect();
		
		for (Database database: context.getActiveDatabases())
		{
			Connection connection = context.getConnection(database);
			
			connection.setAutoCommit(false);
			connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
			
			Statement statement = connection.createStatement();
			
			for (TableProperties table: tables)
			{
				String sql = lockTableSQLMap.get(table);
				
				if (sql == null)
				{
					sql = dialect.getLockTableSQL(table);
					
					logger.debug(sql);
						
					lockTableSQLMap.put(table, sql);
				}
					
				statement.execute(sql);
			}
			
			statement.close();
		}
	}
	
	public void unlock(SynchronizationContext context)
	{
		for (Database database: context.getActiveDatabases())
		{
			try
			{
				Connection connection = context.getConnection(database);
				
				connection.rollback();
				connection.setAutoCommit(true);
			}
			catch (java.sql.SQLException e)
			{
				logger.warn(e.toString(), e);
			}
		}
	}
	
	public void dropUniqueConstraints(SynchronizationContext context, TableProperties table) throws SQLException
	{
		Collection<UniqueConstraint> constraints = table.getUniqueConstraints();
		
		constraints.remove(table.getPrimaryKey());

		Dialect dialect = context.getDialect();

		Connection connection = context.getConnection(context.getTargetDatabase());
		
		Statement statement = connection.createStatement();
		
		// Drop unique constraints on the current table
		for (UniqueConstraint constraint: constraints)
		{
			String sql = dialect.getDropUniqueConstraintSQL(constraint);
			
			logger.debug(sql);
			
			statement.addBatch(sql);
		}
		
		statement.executeBatch();
		statement.close();
	}
	
	public void restoreUniqueConstraints(SynchronizationContext context, TableProperties table) throws SQLException
	{
		Collection<UniqueConstraint> constraints = table.getUniqueConstraints();
		
		constraints.remove(table.getPrimaryKey());

		Dialect dialect = context.getDialect();

		Connection connection = context.getConnection(context.getTargetDatabase());
		
		Statement statement = connection.createStatement();
		
		// Drop unique constraints on the current table
		for (UniqueConstraint constraint: constraints)
		{
			String sql = dialect.getCreateUniqueConstraintSQL(constraint);
			
			logger.debug(sql);
			
			statement.addBatch(sql);
		}
		
		statement.executeBatch();
		statement.close();
	}
	
	public void rollback(Connection connection)
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
	
	public Object getObject(ResultSet resultSet, int index, int type) throws SQLException
	{
		switch (type)
		{
			case Types.BLOB:
			{
				return resultSet.getBlob(index);
			}
			case Types.CLOB:
			{
				return resultSet.getClob(index);
			}
			default:
			{
				return resultSet.getObject(index);
			}
		}
	}
}
