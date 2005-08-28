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
package net.sf.hajdbc.sql;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import net.sf.hajdbc.SQLObject;
import net.sf.hajdbc.Database;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class Statement extends SQLObject implements java.sql.Statement
{
	private static final Pattern SELECT_FOR_UPDATE_PATTERN = Pattern.compile("SELECT\\s+.+\\s+FOR\\s+UPDATE");
	
	/**
	 * Constructs a new StatementProxy.
	 * @param connection a Connection proxy
	 * @param operation an operation that creates Statements
	 * @throws SQLException if operation execution fails
	 */
	public Statement(Connection connection, ConnectionOperation operation) throws SQLException
	{
		super(connection, operation);
	}

	/**
	 * @see net.sf.hajdbc.SQLObject#handleExceptions(java.util.Map)
	 */
	public void handleExceptions(Map exceptionMap) throws SQLException
	{
		if (this.getAutoCommit())
		{
			super.handleExceptions(exceptionMap);
		}
		else
		{
			// If auto-commit is off, give client the opportunity to rollback the transaction
			Iterator exceptionMapEntries = exceptionMap.entrySet().iterator();
			SQLException exception = null;
			
			while (exceptionMapEntries.hasNext())
			{
				Map.Entry exceptionMapEntry = (Map.Entry) exceptionMapEntries.next();
				Database database = (Database) exceptionMapEntry.getKey();
				SQLException cause = (SQLException) exceptionMapEntry.getValue();
				
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
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
			{
				statement.addBatch(sql);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.Statement#cancel()
	 */
	public void cancel() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
			{
				statement.cancel();
				
				return null;
			}
		};
		
		this.executeWriteToDatabase(operation);
	}

	/**
	 * @see java.sql.Statement#clearBatch()
	 */
	public void clearBatch() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
			{
				statement.clearBatch();
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.Statement#clearWarnings()
	 */
	public void clearWarnings() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
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
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
			{
				statement.close();
				
				return null;
			}
		};
		
		this.executeWriteToDatabase(operation);
	}

	/**
	 * @see java.sql.Statement#execute(java.lang.String)
	 */
	public boolean execute(final String sql) throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
			{
				return new Boolean(statement.execute(sql));
			}
		};
		
		return ((Boolean) this.firstValue(this.executeWriteToDatabase(operation))).booleanValue();
	}

	/**
	 * @see java.sql.Statement#execute(java.lang.String, int)
	 */
	public boolean execute(final String sql, final int autoGeneratedKeys) throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
			{
				return new Boolean(statement.execute(sql, autoGeneratedKeys));
			}
		};
		
		return ((Boolean) this.firstValue(this.executeWriteToDatabase(operation))).booleanValue();
	}

	/**
	 * @see java.sql.Statement#execute(java.lang.String, int[])
	 */
	public boolean execute(final String sql, final int[] columnIndexes) throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
			{
				return new Boolean(statement.execute(sql, columnIndexes));
			}
		};
		
		return ((Boolean) this.firstValue(this.executeWriteToDatabase(operation))).booleanValue();
	}

	/**
	 * @see java.sql.Statement#execute(java.lang.String, java.lang.String[])
	 */
	public boolean execute(final String sql, final String[] columnNames) throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
			{
				return new Boolean(statement.execute(sql, columnNames));
			}
		};
		
		return ((Boolean) this.firstValue(this.executeWriteToDatabase(operation))).booleanValue();
	}

	/**
	 * @see java.sql.Statement#executeBatch()
	 */
	public int[] executeBatch() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
			{
				return statement.executeBatch();
			}
		};
		
		return (int[]) this.firstValue(this.executeWriteToDatabase(operation));
	}

	/**
	 * @see java.sql.Statement#executeQuery(java.lang.String)
	 */
	public java.sql.ResultSet executeQuery(final String sql) throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
			{
				return statement.executeQuery(sql);
			}
		};

		return ((this.getResultSetConcurrency() == java.sql.ResultSet.CONCUR_READ_ONLY) && !this.isSelectForUpdate(sql)) ? (java.sql.ResultSet) this.executeReadFromDatabase(operation) : new ResultSet(this, operation);
	}

	/**
	 * @see java.sql.Statement#executeUpdate(java.lang.String)
	 */
	public int executeUpdate(final String sql) throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
			{
				return new Integer(statement.executeUpdate(sql));
			}
		};
		
		return ((Integer) this.firstValue(this.executeWriteToDatabase(operation))).intValue();
	}

	/**
	 * @see java.sql.Statement#executeUpdate(java.lang.String, int)
	 */
	public int executeUpdate(final String sql, final int autoGeneratedKeys) throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
			{
				return new Integer(statement.executeUpdate(sql, autoGeneratedKeys));
			}
		};
		
		return ((Integer) this.firstValue(this.executeWriteToDatabase(operation))).intValue();
	}

	/**
	 * @see java.sql.Statement#executeUpdate(java.lang.String, int[])
	 */
	public int executeUpdate(final String sql, final int[] columnIndexes) throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
			{
				return new Integer(statement.executeUpdate(sql, columnIndexes));
			}
		};
		
		return ((Integer) this.firstValue(this.executeWriteToDatabase(operation))).intValue();
	}

	/**
	 * @see java.sql.Statement#executeUpdate(java.lang.String, java.lang.String[])
	 */
	public int executeUpdate(final String sql, final String[] columnNames) throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
			{
				return new Integer(statement.executeUpdate(sql, columnNames));
			}
		};
		
		return ((Integer) this.firstValue(this.executeWriteToDatabase(operation))).intValue();
	}

	/**
	 * @see java.sql.Statement#getConnection()
	 */
	public java.sql.Connection getConnection()
	{
		return (Connection) this.parent;
	}

	/**
	 * @see java.sql.Statement#getFetchDirection()
	 */
	public int getFetchDirection() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
			{
				return new Integer(statement.getFetchDirection());
			}
		};
		
		return ((Integer) this.executeReadFromDriver(operation)).intValue();
	}

	/**
	 * @see java.sql.Statement#getFetchSize()
	 */
	public int getFetchSize() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
			{
				return new Integer(statement.getFetchSize());
			}
		};
		
		return ((Integer) this.executeReadFromDriver(operation)).intValue();
	}

	/**
	 * @see java.sql.Statement#getGeneratedKeys()
	 */
	public java.sql.ResultSet getGeneratedKeys() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
			{
				return statement.getGeneratedKeys();
			}
		};

		return (java.sql.ResultSet) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.Statement#getMaxFieldSize()
	 */
	public int getMaxFieldSize() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
			{
				return new Integer(statement.getMaxFieldSize());
			}
		};
		
		return ((Integer) this.executeReadFromDriver(operation)).intValue();
	}

	/**
	 * @see java.sql.Statement#getMaxRows()
	 */
	public int getMaxRows() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
			{
				return new Integer(statement.getMaxRows());
			}
		};
		
		return ((Integer) this.executeReadFromDriver(operation)).intValue();
	}

	/**
	 * @see java.sql.Statement#getMoreResults()
	 */
	public boolean getMoreResults() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
			{
				return new Boolean(statement.getMoreResults());
			}
		};
		
		return ((Boolean) this.firstValue(this.executeWriteToDriver(operation))).booleanValue();
	}

	/**
	 * @see java.sql.Statement#getMoreResults(int)
	 */
	public boolean getMoreResults(final int current) throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
			{
				return new Boolean(statement.getMoreResults(current));
			}
		};
		
		return ((Boolean) this.firstValue(this.executeWriteToDriver(operation))).booleanValue();
	}

	/**
	 * @see java.sql.Statement#getQueryTimeout()
	 */
	public int getQueryTimeout() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
			{
				return new Integer(statement.getQueryTimeout());
			}
		};
		
		return ((Integer) this.executeReadFromDriver(operation)).intValue();
	}

	/**
	 * @see java.sql.Statement#getResultSet()
	 */
	public java.sql.ResultSet getResultSet() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
			{
				return statement.getResultSet();
			}
		};

		return (this.getResultSetConcurrency() == java.sql.ResultSet.CONCUR_READ_ONLY) ? (java.sql.ResultSet) this.executeReadFromDriver(operation) : new ResultSet(this, operation);
	}

	/**
	 * @see java.sql.Statement#getResultSetConcurrency()
	 */
	public int getResultSetConcurrency() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
			{
				return new Integer(statement.getResultSetConcurrency());
			}
		};
		
		return ((Integer) this.executeReadFromDriver(operation)).intValue();
	}

	/**
	 * @see java.sql.Statement#getResultSetHoldability()
	 */
	public int getResultSetHoldability() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
			{
				return new Integer(statement.getResultSetHoldability());
			}
		};
		
		return ((Integer) this.executeReadFromDriver(operation)).intValue();
	}

	/**
	 * @see java.sql.Statement#getResultSetType()
	 */
	public int getResultSetType() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
			{
				return new Integer(statement.getResultSetType());
			}
		};
		
		return ((Integer) this.executeReadFromDriver(operation)).intValue();
	}

	/**
	 * @see java.sql.Statement#getUpdateCount()
	 */
	public int getUpdateCount() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
			{
				return new Integer(statement.getUpdateCount());
			}
		};
		
		return ((Integer) this.executeReadFromDriver(operation)).intValue();
	}

	/**
	 * @see java.sql.Statement#getWarnings()
	 */
	public SQLWarning getWarnings() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
			{
				return statement.getWarnings();
			}
		};

		return (SQLWarning) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.Statement#setCursorName(java.lang.String)
	 */
	public void setCursorName(final String name) throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
			{
				statement.setCursorName(name);
				
				return null;
			}
		};
		
		this.executeWriteToDatabase(operation);
		
		this.record(operation);
	}

	/**
	 * @see java.sql.Statement#setEscapeProcessing(boolean)
	 */
	public void setEscapeProcessing(final boolean enable) throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
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
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
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
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
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
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
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
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
			{
				statement.setMaxRows(rows);
				
				return null;
			}
		};
		
		this.executeWriteToDatabase(operation);
		
		this.record(operation);
	}
	
	/**
	 * @see java.sql.Statement#setQueryTimeout(int)
	 */
	public void setQueryTimeout(final int seconds) throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(java.sql.Statement statement) throws SQLException
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
	
	/**
	 * Determines whether or not the specified query is a SELECT...FOR UPDATE query.
	 * These queries always need to be distributed to each node in the cluster.
	 * @param sql a SQL query
	 * @return true if the specified query is a SELECT...FOR UPDATE query, false otherwise.
	 */
	protected boolean isSelectForUpdate(String sql)
	{
		return SELECT_FOR_UPDATE_PATTERN.matcher(sql.toUpperCase()).find();
	}
}
