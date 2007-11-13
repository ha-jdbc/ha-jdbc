/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
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
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.SequenceProperties;
import net.sf.hajdbc.SynchronizationContext;
import net.sf.hajdbc.TableProperties;
import net.sf.hajdbc.UniqueConstraint;
import net.sf.hajdbc.util.SQLExceptionFactory;
import net.sf.hajdbc.util.Strings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Paul Ferraro
 *
 */
public final class SynchronizationSupport
{
	private static Logger logger = LoggerFactory.getLogger(SynchronizationSupport.class);
	
	private SynchronizationSupport()
	{
		// Hide
	}
	
	/**
	 * Drop all foreign key constraints on the target database
	 * @param context a synchronization context
	 * @throws SQLException if database error occurs
	 */
	public static <D> void dropForeignKeys(SynchronizationContext<D> context) throws SQLException
	{
		Collection<TableProperties> tables = context.getDatabaseProperties().getTables();
		
		Dialect dialect = context.getDialect();
		
		Connection connection = context.getConnection(context.getTargetDatabase());
		
		Statement statement = connection.createStatement();
		
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
	
	/**
	 * Restores all foreign key constraints on the target database
	 * @param context a synchronization context
	 * @throws SQLException if database error occurs
	 */
	public static <D> void restoreForeignKeys(SynchronizationContext<D> context) throws SQLException
	{
		Collection<TableProperties> tables = context.getDatabaseProperties().getTables();
		
		Dialect dialect = context.getDialect();
		
		Connection connection = context.getConnection(context.getTargetDatabase());
		
		Statement statement = connection.createStatement();
		
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
	
	/**
	 * Synchronizes the sequences on the target database with the source database.
	 * @param context a synchronization context
	 * @throws SQLException if database error occurs
	 */
	public static <D> void synchronizeSequences(final SynchronizationContext<D> context) throws SQLException
	{
		Collection<SequenceProperties> sequences = context.getDatabaseProperties().getSequences();

		if (!sequences.isEmpty())
		{
			Database<D> sourceDatabase = context.getSourceDatabase();
			
			Set<Database<D>> databases = context.getActiveDatabaseSet();

			ExecutorService executor = context.getExecutor();
			
			Dialect dialect = context.getDialect();
			
			Map<SequenceProperties, Long> sequenceMap = new HashMap<SequenceProperties, Long>();
			Map<Database<D>, Future<Long>> futureMap = new HashMap<Database<D>, Future<Long>>();

			for (SequenceProperties sequence: sequences)
			{
				final String sql = dialect.getNextSequenceValueSQL(sequence);
				
				logger.debug(sql);

				for (final Database<D> database: databases)
				{
					Callable<Long> task = new Callable<Long>()
					{
						public Long call() throws SQLException
						{
							Statement statement = context.getConnection(database).createStatement();
							ResultSet resultSet = statement.executeQuery(sql);
							
							resultSet.next();
							
							long value = resultSet.getLong(1);
							
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
					
					for (Database<D> database: databases)
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
					throw SQLExceptionFactory.createSQLException(e);
				}
				catch (ExecutionException e)
				{
					throw SQLExceptionFactory.createSQLException(e.getCause());
				}
			}
			
			Connection targetConnection = context.getConnection(context.getTargetDatabase());
			Statement targetStatement = targetConnection.createStatement();

			for (SequenceProperties sequence: sequences)
			{
				String sql = dialect.getAlterSequenceSQL(sequence, sequenceMap.get(sequence) + 1);
				
				logger.debug(sql);
				
				targetStatement.addBatch(sql);
			}
			
			targetStatement.executeBatch();		
			targetStatement.close();
		}
	}
	
	public static <D> void synchronizeIdentityColumns(SynchronizationContext<D> context) throws SQLException
	{
		Statement sourceStatement = context.getConnection(context.getSourceDatabase()).createStatement();
		Statement targetStatement = context.getConnection(context.getTargetDatabase()).createStatement();
		
		Dialect dialect = context.getDialect();
		
		for (TableProperties table: context.getDatabaseProperties().getTables())
		{
			Collection<String> columns = table.getIdentityColumns();
			
			if (!columns.isEmpty())
			{
				String selectSQL = MessageFormat.format("SELECT max({0}) FROM {1}", Strings.join(columns, "), max("), table.getName()); //$NON-NLS-1$ //$NON-NLS-2$
				
				logger.debug(selectSQL);
				
				Map<String, Long> map = new HashMap<String, Long>();
				
				ResultSet resultSet = sourceStatement.executeQuery(selectSQL);
				
				if (resultSet.next())
				{
					int i = 0;
					
					for (String column: columns)
					{
						map.put(column, resultSet.getLong(++i));
					}
				}
				
				resultSet.close();
				
				if (!map.isEmpty())
				{
					for (Map.Entry<String, Long> mapEntry: map.entrySet())
					{
						String alterSQL = dialect.getAlterIdentityColumnSQL(table, table.getColumnProperties(mapEntry.getKey()), mapEntry.getValue() + 1);
						
						if (alterSQL != null)
						{
							logger.debug(alterSQL);
							
							targetStatement.addBatch(alterSQL);
						}
					}
					
					targetStatement.executeBatch();
				}
			}
		}
		
		sourceStatement.close();
		targetStatement.close();
	}
	
	/**
	 * Read-locks all of the tables in each active database.
	 * @param context a synchronization context
	 * @throws SQLException if database error occurs
	 */
	public static <D> void lock(final SynchronizationContext<D> context) throws SQLException
	{
		logger.info(Messages.getMessage(Messages.TABLE_LOCK_ACQUIRE));
		
		Set<Database<D>> databases = context.getActiveDatabaseSet();
		
		ExecutorService executor = context.getExecutor();
		
		Collection<Future<Void>> futures = new ArrayList<Future<Void>>(databases.size());
		
		// Create connections and set transaction isolation level
		for (final Database<D> database: databases)
		{
			Callable<Void> task = new Callable<Void>()
			{
				public Void call() throws SQLException
				{
					Connection connection = context.getConnection(database);
					
					connection.setAutoCommit(false);
					connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
					
					return null;
				}
			};
			
			futures.add(executor.submit(task));
		}
		
		Collection<TableProperties> tables = context.getDatabaseProperties().getTables();
		
		try
		{
			for (Future<Void> future: futures)
			{
				future.get();
			}
		}
		catch (InterruptedException e)
		{
			throw SQLExceptionFactory.createSQLException(e);
		}
		catch (ExecutionException e)
		{
			throw SQLExceptionFactory.createSQLException(e.getCause());
		}
		
		futures.clear();
		
		Dialect dialect = context.getDialect();
		
		// For each table - execute a lock table statement
		for (TableProperties table: tables)
		{
			final String sql = dialect.getLockTableSQL(table);
			
			for (final Database<D> database: databases)
			{
				Callable<Void> task = new Callable<Void>()
				{
					public Void call() throws SQLException
					{
						Connection connection = context.getConnection(database);
						
						Statement statement = connection.createStatement();
						
						statement.execute(sql);
						
						statement.close();
						
						return null;
					}
				};
				
				futures.add(executor.submit(task));
			}
			
			try
			{
				for (Future<Void> future: futures)
				{
					future.get();
				}
			}
			catch (InterruptedException e)
			{
				throw SQLExceptionFactory.createSQLException(e);
			}
			catch (ExecutionException e)
			{
				throw SQLExceptionFactory.createSQLException(e.getCause());
			}
		}
	}
	
	public static <D> void unlock(final SynchronizationContext<D> context)
	{
		Set<Database<D>> databases = context.getActiveDatabaseSet();
		
		ExecutorService executor = context.getExecutor();
		
		Collection<Future<Void>> futures = new ArrayList<Future<Void>>(databases.size());
		
		for (final Database<D> database: databases)
		{
			Callable<Void> task = new Callable<Void>()
			{
				public Void call() throws SQLException
				{
					Connection connection = context.getConnection(database);
					
					SynchronizationSupport.rollback(connection);
					
					return null;
				}
			};
			
			futures.add(executor.submit(task));
		}
		
		for (Future<Void> future: futures)
		{
			try
			{
				future.get();
			}
			catch (InterruptedException e)
			{
				Thread.currentThread().interrupt();
				
				logger.warn(e.getMessage(), e);
			}
			catch (ExecutionException e)
			{
				logger.warn(e.getMessage(), e.getCause());
			}
		}
	}
	
	public static <D> void dropUniqueConstraints(SynchronizationContext<D> context, TableProperties table) throws SQLException
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
	
	public static <D> void restoreUniqueConstraints(SynchronizationContext<D> context, TableProperties table) throws SQLException
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
	
	public static void rollback(Connection connection)
	{
		try
		{
			connection.rollback();
			connection.setAutoCommit(true);
		}
		catch (SQLException e)
		{
			logger.warn(e.toString(), e);
		}
	}
	
	public static Object getObject(ResultSet resultSet, int index, int type) throws SQLException
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
