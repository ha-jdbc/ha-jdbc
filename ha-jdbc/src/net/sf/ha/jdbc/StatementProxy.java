package net.sf.ha.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Map;

/**
 * @author Paul Ferraro
 * @version $Revision$
 */
public class StatementProxy extends SQLProxy implements Statement
{
	protected ConnectionProxy connection;

	/**
	 * Constructs a new StatementProxy.
	 * @param connection
	 * @param statementMap
	 */
	public StatementProxy(ConnectionProxy connection, Map statementMap)
	{
		super(statementMap);
		
		this.connection = connection;
	}
	
	/**
	 * @see java.sql.Statement#getFetchDirection()
	 */
	public int getFetchDirection() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				return new Integer(statement.getFetchDirection());
			}
		};
		
		return ((Integer) this.executeGet(operation)).intValue();
	}

	/**
	 * @see java.sql.Statement#getFetchSize()
	 */
	public int getFetchSize() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				return new Integer(statement.getFetchSize());
			}
		};
		
		return ((Integer) this.executeGet(operation)).intValue();
	}

	/**
	 * @see java.sql.Statement#getMaxFieldSize()
	 */
	public int getMaxFieldSize() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				return new Integer(statement.getMaxFieldSize());
			}
		};
		
		return ((Integer) this.executeGet(operation)).intValue();
	}

	/**
	 * @see java.sql.Statement#getMaxRows()
	 */
	public int getMaxRows() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				return new Integer(statement.getMaxRows());
			}
		};
		
		return ((Integer) this.executeGet(operation)).intValue();
	}

	/**
	 * @see java.sql.Statement#getQueryTimeout()
	 */
	public int getQueryTimeout() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				return new Integer(statement.getQueryTimeout());
			}
		};
		
		return ((Integer) this.executeGet(operation)).intValue();
	}

	/**
	 * @see java.sql.Statement#getResultSetConcurrency()
	 */
	public int getResultSetConcurrency() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				return new Integer(statement.getResultSetConcurrency());
			}
		};
		
		return ((Integer) this.executeGet(operation)).intValue();
	}

	/**
	 * @see java.sql.Statement#getResultSetHoldability()
	 */
	public int getResultSetHoldability() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				return new Integer(statement.getResultSetHoldability());
			}
		};
		
		return ((Integer) this.executeGet(operation)).intValue();
	}

	/**
	 * @see java.sql.Statement#getResultSetType()
	 */
	public int getResultSetType() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				return new Integer(statement.getResultSetType());
			}
		};
		
		return ((Integer) this.executeGet(operation)).intValue();
	}

	/**
	 * @see java.sql.Statement#getUpdateCount()
	 */
	public int getUpdateCount() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				return new Integer(statement.getUpdateCount());
			}
		};
		
		return ((Integer) this.executeGet(operation)).intValue();
	}

	/**
	 * @see java.sql.Statement#cancel()
	 */
	public void cancel() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				statement.cancel();
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.Statement#clearBatch()
	 */
	public void clearBatch() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				statement.clearBatch();
				
				return null;
			}
		};
		
		this.executeSet(operation);
	}

	/**
	 * @see java.sql.Statement#clearWarnings()
	 */
	public void clearWarnings() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				statement.clearWarnings();
				
				return null;
			}
		};
		
		this.executeSet(operation);
	}

	/**
	 * @see java.sql.Statement#close()
	 */
	public void close() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				statement.close();
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.Statement#getMoreResults()
	 */
	public boolean getMoreResults() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				return new Boolean(statement.getMoreResults());
			}
		};
		
		return ((Boolean) this.firstItem(this.executeSet(operation))).booleanValue();
	}

	/**
	 * @see java.sql.Statement#executeBatch()
	 */
	public int[] executeBatch() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				return statement.executeBatch();
			}
		};
		
		return (int[]) this.firstItem(this.executeWrite(operation));
	}

	/**
	 * @see java.sql.Statement#setFetchDirection(int)
	 */
	public void setFetchDirection(final int direction) throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				statement.setFetchDirection(direction);
				
				return null;
			}
		};
		
		this.executeSet(operation);
	}

	/**
	 * @see java.sql.Statement#setFetchSize(int)
	 */
	public void setFetchSize(final int size) throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				statement.setFetchSize(size);
				
				return null;
			}
		};
		
		this.executeSet(operation);
	}

	/**
	 * @see java.sql.Statement#setMaxFieldSize(int)
	 */
	public void setMaxFieldSize(final int size) throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				statement.setMaxFieldSize(size);
				
				return null;
			}
		};
		
		this.executeSet(operation);
	}

	/**
	 * @see java.sql.Statement#setMaxRows(int)
	 */
	public void setMaxRows(final int rows) throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				statement.setMaxRows(rows);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.Statement#setQueryTimeout(int)
	 */
	public void setQueryTimeout(final int seconds) throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				statement.setQueryTimeout(seconds);
				
				return null;
			}
		};
		
		this.executeSet(operation);
	}

	/**
	 * @see java.sql.Statement#getMoreResults(int)
	 */
	public boolean getMoreResults(final int current) throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				return new Boolean(statement.getMoreResults(current));
			}
		};
		
		return ((Boolean) this.firstItem(this.executeSet(operation))).booleanValue();
	}

	/**
	 * @see java.sql.Statement#setEscapeProcessing(boolean)
	 */
	public void setEscapeProcessing(final boolean enable) throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				statement.setEscapeProcessing(enable);
				
				return null;
			}
		};
		
		this.executeSet(operation);
	}

	/**
	 * @see java.sql.Statement#executeUpdate(java.lang.String)
	 */
	public int executeUpdate(final String sql) throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				return new Integer(statement.executeUpdate(sql));
			}
		};
		
		return ((Integer) this.firstItem(this.executeWrite(operation))).intValue();
	}

	/**
	 * @see java.sql.Statement#addBatch(java.lang.String)
	 */
	public void addBatch(final String sql) throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				statement.addBatch(sql);
				
				return null;
			}
		};
		
		this.executeSet(operation);
	}

	/**
	 * @see java.sql.Statement#setCursorName(java.lang.String)
	 */
	public void setCursorName(final String name) throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				statement.setCursorName(name);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see java.sql.Statement#execute(java.lang.String)
	 */
	public boolean execute(final String sql) throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				return new Boolean(statement.execute(sql));
			}
		};
		
		return ((Boolean) this.firstItem(this.executeWrite(operation))).booleanValue();
	}

	/**
	 * @see java.sql.Statement#executeUpdate(java.lang.String, int)
	 */
	public int executeUpdate(final String sql, final int autoGeneratedKeys) throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				return new Integer(statement.executeUpdate(sql, autoGeneratedKeys));
			}
		};
		
		return ((Integer) this.firstItem(this.executeWrite(operation))).intValue();
	}

	/**
	 * @see java.sql.Statement#execute(java.lang.String, int)
	 */
	public boolean execute(final String sql, final int autoGeneratedKeys) throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				return new Boolean(statement.execute(sql, autoGeneratedKeys));
			}
		};
		
		return ((Boolean) this.firstItem(this.executeWrite(operation))).booleanValue();
	}

	/**
	 * @see java.sql.Statement#executeUpdate(java.lang.String, int[])
	 */
	public int executeUpdate(final String sql, final int[] columnIndexes) throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				return new Integer(statement.executeUpdate(sql, columnIndexes));
			}
		};
		
		return ((Integer) this.firstItem(this.executeWrite(operation))).intValue();
	}

	/**
	 * @see java.sql.Statement#execute(java.lang.String, int[])
	 */
	public boolean execute(final String sql, final int[] columnIndexes) throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				return new Boolean(statement.execute(sql, columnIndexes));
			}
		};
		
		return ((Boolean) this.firstItem(this.executeWrite(operation))).booleanValue();
	}

	/**
	 * @see java.sql.Statement#getConnection()
	 */
	public Connection getConnection()
	{
		return this.connection;
	}

	/**
	 * @see java.sql.Statement#getGeneratedKeys()
	 */
	public ResultSet getGeneratedKeys() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				return statement.getGeneratedKeys();
			}
		};

		return (ResultSet) this.executeGet(operation);
	}

	/**
	 * @see java.sql.Statement#getResultSet()
	 */
	public ResultSet getResultSet() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				return statement.getResultSet();
			}
		};

		return (this.getResultSetConcurrency() == ResultSet.CONCUR_READ_ONLY) ? (ResultSet) this.executeGet(operation) : new ResultSetProxy(this, this.executeSet(operation));
	}

	/**
	 * @see java.sql.Statement#getWarnings()
	 */
	public SQLWarning getWarnings() throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				return statement.getWarnings();
			}
		};

		return (SQLWarning) this.executeGet(operation);
	}

	/**
	 * @see java.sql.Statement#executeUpdate(java.lang.String, java.lang.String[])
	 */
	public int executeUpdate(final String sql, final String[] columnNames) throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				return new Integer(statement.executeUpdate(sql, columnNames));
			}
		};
		
		return ((Integer) this.firstItem(this.executeWrite(operation))).intValue();
	}

	/**
	 * @see java.sql.Statement#execute(java.lang.String, java.lang.String[])
	 */
	public boolean execute(final String sql, final String[] columnNames) throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				return new Boolean(statement.execute(sql, columnNames));
			}
		};
		
		return ((Boolean) this.firstItem(this.executeWrite(operation))).booleanValue();
	}

	/**
	 * @see java.sql.Statement#executeQuery(java.lang.String)
	 */
	public ResultSet executeQuery(final String sql) throws SQLException
	{
		StatementOperation operation = new StatementOperation()
		{
			public Object execute(Statement statement) throws SQLException
			{
				return statement.executeQuery(sql);
			}
		};

		return (this.getResultSetConcurrency() == ResultSet.CONCUR_READ_ONLY) ? (ResultSet) this.executeRead(operation) : new ResultSetProxy(this, this.executeWrite(operation));
	}
	
	/**
	 * @see net.sf.ha.jdbc.SQLProxy#getDatabaseCluster()
	 */
	protected DatabaseCluster getDatabaseCluster()
	{
		return this.connection.getDatabaseCluster();
	}
}
