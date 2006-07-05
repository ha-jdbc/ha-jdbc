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
package net.sf.hajdbc.sql;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.LockManager;
import net.sf.hajdbc.Operation;
import net.sf.hajdbc.SQLObject;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @param <T> 
 * @since   1.0
 */
public class Statement<T extends java.sql.Statement> extends SQLObject<T, java.sql.Connection> implements java.sql.Statement
{
	private List<String> sqlList = new ArrayList<String>();
	
	/**
	 * Constructs a new StatementProxy.
	 * @param connection a Connection proxy
	 * @param operation an operation that creates Statements
	 * @throws SQLException if operation execution fails
	 */
	public Statement(Connection<?> connection, Operation<java.sql.Connection, T> operation) throws SQLException
	{
		super(connection, operation, connection.getDatabaseCluster().getNonTransactionalExecutor());
	}

	/**
	 * @see net.sf.hajdbc.SQLObject#handleExceptions(java.util.Map)
	 */
	@Override
	public void handleExceptions(Map<Database, SQLException> exceptionMap) throws SQLException
	{
		if (this.getAutoCommit())
		{
			super.handleExceptions(exceptionMap);
		}
		else
		{
			// If auto-commit is off, give client the opportunity to rollback the transaction
			SQLException exception = null;
			
			for (Map.Entry<Database, SQLException> exceptionMapEntry: exceptionMap.entrySet())
			{
				Database database = exceptionMapEntry.getKey();
				SQLException cause = exceptionMapEntry.getValue();
				
				try
				{
					this.getDatabaseCluster().handleFailure(database, cause);
				}
				catch (SQLException e)
				{
					if (exception == null)
					{
						exception = e;
					}
					else
					{
						exception.setNextException(e);
					}
				}
			}
			
			if (exception != null)
			{
				throw exception;
			}
		}
	}
	
	/**
	 * @see java.sql.Statement#addBatch(java.lang.String)
	 */
	public void addBatch(final String sql) throws SQLException
	{
		Operation<T, Void> operation = new Operation<T, Void>()
		{
			public Void execute(Database database, T statement) throws SQLException
			{
				statement.addBatch(sql);
				
				return null;
			}
		};
		
		this.sqlList.add(sql);
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.Statement#cancel()
	 */
	public void cancel() throws SQLException
	{
		Operation<T, Void> operation = new Operation<T, Void>()
		{
			public Void execute(Database database, T statement) throws SQLException
			{
				statement.cancel();
				
				return null;
			}
		};
		
		this.executeNonTransactionalWriteToDatabase(operation);
	}

	/**
	 * @see java.sql.Statement#clearBatch()
	 */
	public void clearBatch() throws SQLException
	{
		Operation<T, Void> operation = new Operation<T, Void>()
		{
			public Void execute(Database database, T statement) throws SQLException
			{
				statement.clearBatch();
				
				return null;
			}
		};
		
		this.sqlList.clear();
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.Statement#clearWarnings()
	 */
	public void clearWarnings() throws SQLException
	{
		Operation<T, Void> operation = new Operation<T, Void>()
		{
			public Void execute(Database database, T statement) throws SQLException
			{
				statement.clearWarnings();
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.Statement#close()
	 */
	public void close() throws SQLException
	{
		Operation<T, Void> operation = new Operation<T, Void>()
		{
			public Void execute(Database database, T statement) throws SQLException
			{
				statement.close();
				
				return null;
			}
		};
		
		this.executeNonTransactionalWriteToDatabase(operation);
	}

	/**
	 * @see java.sql.Statement#execute(java.lang.String)
	 */
	public boolean execute(final String sql) throws SQLException
	{
		Operation<T, Boolean> operation = new Operation<T, Boolean>()
		{
			public Boolean execute(Database database, T statement) throws SQLException
			{
				return statement.execute(sql);
			}
		};
		
		return this.firstValue(this.executeTransactionalWriteToDatabase(operation, this.getLock(sql)));
	}

	/**
	 * @see java.sql.Statement#execute(java.lang.String, int)
	 */
	public boolean execute(final String sql, final int autoGeneratedKeys) throws SQLException
	{
		Operation<T, Boolean> operation = new Operation<T, Boolean>()
		{
			public Boolean execute(Database database, T statement) throws SQLException
			{
				return statement.execute(sql, autoGeneratedKeys);
			}
		};
		
		return this.firstValue(this.executeTransactionalWriteToDatabase(operation, this.getLock(sql)));
	}

	/**
	 * @see java.sql.Statement#execute(java.lang.String, int[])
	 */
	public boolean execute(final String sql, final int[] columnIndexes) throws SQLException
	{
		Operation<T, Boolean> operation = new Operation<T, Boolean>()
		{
			public Boolean execute(Database database, T statement) throws SQLException
			{
				return statement.execute(sql, columnIndexes);
			}
		};
		
		return this.firstValue(this.executeTransactionalWriteToDatabase(operation, this.getLock(sql)));
	}

	/**
	 * @see java.sql.Statement#execute(java.lang.String, java.lang.String[])
	 */
	public boolean execute(final String sql, final String[] columnNames) throws SQLException
	{
		Operation<T, Boolean> operation = new Operation<T, Boolean>()
		{
			public Boolean execute(Database database, T statement) throws SQLException
			{
				return statement.execute(sql, columnNames);
			}
		};
		
		return this.firstValue(this.executeTransactionalWriteToDatabase(operation, this.getLock(sql)));
	}

	/**
	 * @see java.sql.Statement#executeBatch()
	 */
	public int[] executeBatch() throws SQLException
	{
		Operation<T, int[]> operation = new Operation<T, int[]>()
		{
			public int[] execute(Database database, T statement) throws SQLException
			{
				return statement.executeBatch();
			}
		};
				
		return this.firstValue(this.executeTransactionalWriteToDatabase(operation, this.getLockList(this.sqlList)));
	}

	/**
	 * @see java.sql.Statement#executeQuery(java.lang.String)
	 */
	public java.sql.ResultSet executeQuery(final String sql) throws SQLException
	{
		Operation<T, java.sql.ResultSet> operation = new Operation<T, java.sql.ResultSet>()
		{
			public java.sql.ResultSet execute(Database database, T statement) throws SQLException
			{
				return statement.executeQuery(sql);
			}
		};
		
		Lock lock = this.getLock(sql);
		
		return ((lock == null) && (this.getResultSetConcurrency() == java.sql.ResultSet.CONCUR_READ_ONLY) && !this.isSelectForUpdate(sql)) ? this.executeReadFromDatabase(operation) : new ResultSet<T>(this, operation, lock);
	}

	/**
	 * @see java.sql.Statement#executeUpdate(java.lang.String)
	 */
	public int executeUpdate(final String sql) throws SQLException
	{
		Operation<T, Integer> operation = new Operation<T, Integer>()
		{
			public Integer execute(Database database, T statement) throws SQLException
			{
				return statement.executeUpdate(sql);
			}
		};
		
		return this.firstValue(this.executeTransactionalWriteToDatabase(operation, this.getLock(sql)));
	}

	/**
	 * @see java.sql.Statement#executeUpdate(java.lang.String, int)
	 */
	public int executeUpdate(final String sql, final int autoGeneratedKeys) throws SQLException
	{
		Operation<T, Integer> operation = new Operation<T, Integer>()
		{
			public Integer execute(Database database, T statement) throws SQLException
			{
				return statement.executeUpdate(sql, autoGeneratedKeys);
			}
		};
		
		return this.firstValue(this.executeTransactionalWriteToDatabase(operation, this.getLock(sql)));
	}

	/**
	 * @see java.sql.Statement#executeUpdate(java.lang.String, int[])
	 */
	public int executeUpdate(final String sql, final int[] columnIndexes) throws SQLException
	{
		Operation<T, Integer> operation = new Operation<T, Integer>()
		{
			public Integer execute(Database database, T statement) throws SQLException
			{
				return statement.executeUpdate(sql, columnIndexes);
			}
		};
		
		return this.firstValue(this.executeTransactionalWriteToDatabase(operation, this.getLock(sql)));
	}

	/**
	 * @see java.sql.Statement#executeUpdate(java.lang.String, java.lang.String[])
	 */
	public int executeUpdate(final String sql, final String[] columnNames) throws SQLException
	{
		Operation<T, Integer> operation = new Operation<T, Integer>()
		{
			public Integer execute(Database database, T statement) throws SQLException
			{
				return statement.executeUpdate(sql, columnNames);
			}
		};
		
		return this.firstValue(this.executeTransactionalWriteToDatabase(operation, this.getLock(sql)));
	}

	/**
	 * @see java.sql.Statement#getConnection()
	 */
	public java.sql.Connection getConnection()
	{
		return Connection.class.cast(this.parent);
	}

	/**
	 * @see java.sql.Statement#getFetchDirection()
	 */
	public int getFetchDirection() throws SQLException
	{
		Operation<T, Integer> operation = new Operation<T, Integer>()
		{
			public Integer execute(Database database, T statement) throws SQLException
			{
				return statement.getFetchDirection();
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.Statement#getFetchSize()
	 */
	public int getFetchSize() throws SQLException
	{
		Operation<T, Integer> operation = new Operation<T, Integer>()
		{
			public Integer execute(Database database, T statement) throws SQLException
			{
				return statement.getFetchSize();
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.Statement#getGeneratedKeys()
	 */
	public java.sql.ResultSet getGeneratedKeys() throws SQLException
	{
		Operation<T, java.sql.ResultSet> operation = new Operation<T, java.sql.ResultSet>()
		{
			public java.sql.ResultSet execute(Database database, T statement) throws SQLException
			{
				return statement.getGeneratedKeys();
			}
		};

		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.Statement#getMaxFieldSize()
	 */
	public int getMaxFieldSize() throws SQLException
	{
		Operation<T, Integer> operation = new Operation<T, Integer>()
		{
			public Integer execute(Database database, T statement) throws SQLException
			{
				return statement.getMaxFieldSize();
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.Statement#getMaxRows()
	 */
	public int getMaxRows() throws SQLException
	{
		Operation<T, Integer> operation = new Operation<T, Integer>()
		{
			public Integer execute(Database database, T statement) throws SQLException
			{
				return statement.getMaxRows();
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.Statement#getMoreResults()
	 */
	public boolean getMoreResults() throws SQLException
	{
		Operation<T, Boolean> operation = new Operation<T, Boolean>()
		{
			public Boolean execute(Database database, T statement) throws SQLException
			{
				return statement.getMoreResults();
			}
		};
		
		return this.firstValue(this.executeNonTransactionalWriteToDatabase(operation));
	}

	/**
	 * @see java.sql.Statement#getMoreResults(int)
	 */
	public boolean getMoreResults(final int current) throws SQLException
	{
		Operation<T, Boolean> operation = new Operation<T, Boolean>()
		{
			public Boolean execute(Database database, T statement) throws SQLException
			{
				return statement.getMoreResults(current);
			}
		};
		
		return this.firstValue((current == KEEP_CURRENT_RESULT) ? this.executeWriteToDriver(operation) : this.executeNonTransactionalWriteToDatabase(operation));
	}

	/**
	 * @see java.sql.Statement#getQueryTimeout()
	 */
	public int getQueryTimeout() throws SQLException
	{
		Operation<T, Integer> operation = new Operation<T, Integer>()
		{
			public Integer execute(Database database, T statement) throws SQLException
			{
				return statement.getQueryTimeout();
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.Statement#getResultSet()
	 */
	public java.sql.ResultSet getResultSet() throws SQLException
	{
		Operation<T, java.sql.ResultSet> operation = new Operation<T, java.sql.ResultSet>()
		{
			public java.sql.ResultSet execute(Database database, T statement) throws SQLException
			{
				return statement.getResultSet();
			}
		};

		return (this.getResultSetConcurrency() == java.sql.ResultSet.CONCUR_READ_ONLY) ? this.executeReadFromDriver(operation) : new ResultSet<T>(this, operation, null);
	}

	/**
	 * @see java.sql.Statement#getResultSetConcurrency()
	 */
	public int getResultSetConcurrency() throws SQLException
	{
		Operation<T, Integer> operation = new Operation<T, Integer>()
		{
			public Integer execute(Database database, T statement) throws SQLException
			{
				return statement.getResultSetConcurrency();
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.Statement#getResultSetHoldability()
	 */
	public int getResultSetHoldability() throws SQLException
	{
		Operation<T, Integer> operation = new Operation<T, Integer>()
		{
			public Integer execute(Database database, T statement) throws SQLException
			{
				return statement.getResultSetHoldability();
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.Statement#getResultSetType()
	 */
	public int getResultSetType() throws SQLException
	{
		Operation<T, Integer> operation = new Operation<T, Integer>()
		{
			public Integer execute(Database database, T statement) throws SQLException
			{
				return statement.getResultSetType();
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.Statement#getUpdateCount()
	 */
	public int getUpdateCount() throws SQLException
	{
		Operation<T, Integer> operation = new Operation<T, Integer>()
		{
			public Integer execute(Database database, T statement) throws SQLException
			{
				return statement.getUpdateCount();
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.Statement#getWarnings()
	 */
	public SQLWarning getWarnings() throws SQLException
	{
		Operation<T, SQLWarning> operation = new Operation<T, SQLWarning>()
		{
			public SQLWarning execute(Database database, T statement) throws SQLException
			{
				return statement.getWarnings();
			}
		};

		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.Statement#setCursorName(java.lang.String)
	 */
	public void setCursorName(final String name) throws SQLException
	{
		Operation<T, Void> operation = new Operation<T, Void>()
		{
			public Void execute(Database database, T statement) throws SQLException
			{
				statement.setCursorName(name);
				
				return null;
			}
		};
		
		this.executeNonTransactionalWriteToDatabase(operation);
		
		this.record(operation);
	}

	/**
	 * @see java.sql.Statement#setEscapeProcessing(boolean)
	 */
	public void setEscapeProcessing(final boolean enable) throws SQLException
	{
		Operation<T, Void> operation = new Operation<T, Void>()
		{
			public Void execute(Database database, T statement) throws SQLException
			{
				statement.setEscapeProcessing(enable);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.Statement#setFetchDirection(int)
	 */
	public void setFetchDirection(final int direction) throws SQLException
	{
		Operation<T, Void> operation = new Operation<T, Void>()
		{
			public Void execute(Database database, T statement) throws SQLException
			{
				statement.setFetchDirection(direction);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.Statement#setFetchSize(int)
	 */
	public void setFetchSize(final int size) throws SQLException
	{
		Operation<T, Void> operation = new Operation<T, Void>()
		{
			public Void execute(Database database, T statement) throws SQLException
			{
				statement.setFetchSize(size);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.Statement#setMaxFieldSize(int)
	 */
	public void setMaxFieldSize(final int size) throws SQLException
	{
		Operation<T, Void> operation = new Operation<T, Void>()
		{
			public Void execute(Database database, T statement) throws SQLException
			{
				statement.setMaxFieldSize(size);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.Statement#setMaxRows(int)
	 */
	public void setMaxRows(final int rows) throws SQLException
	{
		Operation<T, Void> operation = new Operation<T, Void>()
		{
			public Void execute(Database database, T statement) throws SQLException
			{
				statement.setMaxRows(rows);
				
				return null;
			}
		};
		
		this.executeNonTransactionalWriteToDatabase(operation);
		
		this.record(operation);
	}
	
	/**
	 * @see java.sql.Statement#setQueryTimeout(int)
	 */
	public void setQueryTimeout(final int seconds) throws SQLException
	{
		Operation<T, Void> operation = new Operation<T, Void>()
		{
			public Void execute(Database database, T statement) throws SQLException
			{
				statement.setQueryTimeout(seconds);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	private boolean getAutoCommit()
	{
		try
		{
			return this.getConnection().getAutoCommit();
		}
		catch (SQLException e)
		{
			return true;
		}
	}
	
	protected boolean isSelectForUpdate(String sql) throws SQLException
	{
		DatabaseCluster databaseCluster = this.getDatabaseCluster();
		
		return databaseCluster.getDatabaseMetaDataCache().getDatabaseProperties(this.getConnection()).isSelectForUpdateSupported() ? databaseCluster.getDialect().isSelectForUpdate(sql) : false;
	}
	
	protected Lock getLock(String sql) throws SQLException
	{
		DatabaseCluster databaseCluster = this.getDatabaseCluster();
		
		String sequence = databaseCluster.getDialect().parseSequence(sql);
		
		return (sequence != null) ? databaseCluster.getLockManager().writeLock(sequence) : null;
	}
	
	private List<Lock> getLockList(List<String> sqlList) throws SQLException
	{
		DatabaseCluster databaseCluster = this.getDatabaseCluster();
		
		Dialect dialect = databaseCluster.getDialect();
		
		Set<String> sequenceSet = new LinkedHashSet<String>(sqlList.size());
		
		for (String sql: this.sqlList)
		{
			String sequence = dialect.parseSequence(sql);
			
			if (sequence != null)
			{
				sequenceSet.add(sequence);
			}
		}
		
		if (sequenceSet.size() == 0) return java.util.Collections.emptyList();
		
		List<Lock> lockList = new ArrayList<Lock>(sequenceSet.size());

		LockManager lockManager = databaseCluster.getLockManager();
		
		for (String sequence: sequenceSet)
		{
			lockList.add(lockManager.writeLock(sequence));
		}
		
		return lockList;
	}
}
