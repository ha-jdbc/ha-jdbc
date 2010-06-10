/*
 * HA-JDBC: High-Availablity JDBC
 * Copyright 2004-May 12, 2010 Paul Ferraro
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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.MessageFormat;
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
import net.sf.hajdbc.ExceptionType;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.cache.ForeignKeyConstraint;
import net.sf.hajdbc.cache.SequenceProperties;
import net.sf.hajdbc.cache.TableProperties;
import net.sf.hajdbc.cache.UniqueConstraint;
import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;
import net.sf.hajdbc.util.Strings;

/**
 * Default {@link SynchonrizationSupport} implementation.
 * @author Paul Ferraro
 */
public class SynchronizationSupportImpl<Z, D extends Database<Z>> implements SynchronizationSupport
{
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	private final SynchronizationContext<Z, D> context;
	
	public SynchronizationSupportImpl(SynchronizationContext<Z, D> context)
	{
		this.context = context;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sync.SynchronizationSupport#dropForeignKeys()
	 */
	@Override
	public void dropForeignKeys() throws SQLException
	{
		Dialect dialect = this.context.getDialect();
		
		Connection connection = this.context.getConnection(this.context.getTargetDatabase());
		
		Statement statement = connection.createStatement();
		
		for (TableProperties table: this.context.getTargetDatabaseProperties().getTables())
		{
			for (ForeignKeyConstraint constraint: table.getForeignKeyConstraints())
			{
				String sql = dialect.getDropForeignKeyConstraintSQL(constraint);
				
				this.logger.log(Level.DEBUG, sql);
				
				statement.addBatch(sql);
			}
		}
		
		statement.executeBatch();
		statement.close();
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sync.SynchronizationSupport#restoreForeignKeys()
	 */
	@Override
	public void restoreForeignKeys() throws SQLException
	{
		Dialect dialect = this.context.getDialect();
		
		Connection connection = this.context.getConnection(this.context.getTargetDatabase());
		
		Statement statement = connection.createStatement();
		
		for (TableProperties table: this.context.getSourceDatabaseProperties().getTables())
		{
			for (ForeignKeyConstraint constraint: table.getForeignKeyConstraints())
			{
				String sql = dialect.getCreateForeignKeyConstraintSQL(constraint);
				
				this.logger.log(Level.DEBUG, sql);
				
				statement.addBatch(sql);
			}
		}
		
		statement.executeBatch();
		statement.close();
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sync.SynchronizationSupport#synchronizeSequences()
	 */
	@Override
	public void synchronizeSequences() throws SQLException
	{
		Collection<SequenceProperties> sequences = this.context.getSourceDatabaseProperties().getSequences();

		if (!sequences.isEmpty())
		{
			D sourceDatabase = this.context.getSourceDatabase();
			
			Set<D> databases = this.context.getActiveDatabaseSet();

			ExecutorService executor = this.context.getExecutor();
			
			Dialect dialect = this.context.getDialect();
			
			Map<SequenceProperties, Long> sequenceMap = new HashMap<SequenceProperties, Long>();
			Map<D, Future<Long>> futureMap = new HashMap<D, Future<Long>>();

			for (SequenceProperties sequence: sequences)
			{
				final String sql = dialect.getNextSequenceValueSQL(sequence);
				
				this.logger.log(Level.DEBUG, sql);

				for (final D database: databases)
				{
					final SynchronizationContext<Z, D> context = this.context;
					
					Callable<Long> task = new Callable<Long>()
					{
						@Override
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
					
					for (D database: databases)
					{
						if (!database.equals(sourceDatabase))
						{
							Long value = futureMap.get(database).get();
							
							if (!value.equals(sourceValue))
							{
								throw new SQLException(Messages.SEQUENCE_OUT_OF_SYNC.getMessage(sequence, database, value, sourceDatabase, sourceValue));
							}
						}
					}
				}
				catch (InterruptedException e)
				{
					throw ExceptionType.getExceptionFactory(SQLException.class).createException(e);
				}
				catch (ExecutionException e)
				{
					throw ExceptionType.getExceptionFactory(SQLException.class).createException(e.getCause());
				}
			}
			
			Connection targetConnection = this.context.getConnection(this.context.getTargetDatabase());
			Statement targetStatement = targetConnection.createStatement();

			for (SequenceProperties sequence: sequences)
			{
				String sql = dialect.getAlterSequenceSQL(sequence, sequenceMap.get(sequence) + 1);
				
				this.logger.log(Level.DEBUG, sql);
				
				targetStatement.addBatch(sql);
			}
			
			targetStatement.executeBatch();		
			targetStatement.close();
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sync.SynchronizationSupport#synchronizeIdentityColumns()
	 */
	@Override
	public void synchronizeIdentityColumns() throws SQLException
	{
		Statement sourceStatement = this.context.getConnection(this.context.getSourceDatabase()).createStatement();
		Statement targetStatement = this.context.getConnection(this.context.getTargetDatabase()).createStatement();
		
		Dialect dialect = this.context.getDialect();
		
		for (TableProperties table: this.context.getSourceDatabaseProperties().getTables())
		{
			Collection<String> columns = table.getIdentityColumns();
			
			if (!columns.isEmpty())
			{
				String selectSQL = MessageFormat.format("SELECT max({0}) FROM {1}", Strings.join(columns, "), max("), table.getName()); //$NON-NLS-1$ //$NON-NLS-2$
				
				this.logger.log(Level.DEBUG, selectSQL);
				
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
							this.logger.log(Level.DEBUG, alterSQL);
							
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
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sync.SynchronizationSupport#dropUniqueConstraints()
	 */
	@Override
	public void dropUniqueConstraints() throws SQLException
	{
		Dialect dialect = this.context.getDialect();

		Connection connection = this.context.getConnection(this.context.getTargetDatabase());
		
		Statement statement = connection.createStatement();
		
		for (TableProperties table: this.context.getTargetDatabaseProperties().getTables())
		{
			for (UniqueConstraint constraint: table.getUniqueConstraints())
			{
				String sql = dialect.getDropUniqueConstraintSQL(constraint);
				
				this.logger.log(Level.DEBUG, sql);
				
				statement.addBatch(sql);
			}
		}
		
		statement.executeBatch();
		statement.close();
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sync.SynchronizationSupport#restoreUniqueConstraints()
	 */
	@Override
	public void restoreUniqueConstraints() throws SQLException
	{
		Dialect dialect = this.context.getDialect();

		Connection connection = this.context.getConnection(this.context.getTargetDatabase());
		
		Statement statement = connection.createStatement();
		
		for (TableProperties table: this.context.getSourceDatabaseProperties().getTables())
		{
			// Drop unique constraints on the current table
			for (UniqueConstraint constraint: table.getUniqueConstraints())
			{
				String sql = dialect.getCreateUniqueConstraintSQL(constraint);
				
				this.logger.log(Level.DEBUG, sql);
				
				statement.addBatch(sql);
			}
		}
		
		statement.executeBatch();
		statement.close();
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sync.SynchronizationSupport#rollback(java.sql.Connection)
	 */
	@Override
	public void rollback(Connection connection)
	{
		try
		{
			connection.rollback();
		}
		catch (SQLException e)
		{
			this.logger.log(Level.WARN, e);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sync.SynchronizationSupport#getObject(java.sql.ResultSet, int, int)
	 */
	@Override
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
