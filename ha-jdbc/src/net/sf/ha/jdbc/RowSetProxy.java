package net.sf.ha.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import javax.sql.RowSet;
import javax.sql.RowSetListener;

/**
 * @author Paul Ferraro
 * @version $Revision$
 */
public class RowSetProxy extends ResultSetProxy implements RowSet
{
	/**
	 * Constructs a new RowSetProxy
	 * @param statement
	 * @param resultSetMap
	 */
	public RowSetProxy(StatementProxy statement, Map resultSetMap)
	{
		super(statement, resultSetMap);
	}
	
	/**
	 * @see javax.sql.RowSet#getMaxFieldSize()
	 */
	public int getMaxFieldSize() throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				return new Integer(rowSet.getMaxFieldSize());
			}
		};
		
		return ((Integer) this.executeRead(operation)).intValue();
	}

	/**
	 * @see javax.sql.RowSet#getMaxRows()
	 */
	public int getMaxRows() throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				return new Integer(rowSet.getMaxRows());
			}
		};
		
		return ((Integer) this.executeRead(operation)).intValue();
	}

	/**
	 * @see javax.sql.RowSet#getQueryTimeout()
	 */
	public int getQueryTimeout() throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				return new Integer(rowSet.getQueryTimeout());
			}
		};
		
		return ((Integer) this.executeRead(operation)).intValue();
	}

	/**
	 * @see javax.sql.RowSet#getTransactionIsolation()
	 */
	public int getTransactionIsolation()
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet)
			{
				return new Integer(rowSet.getTransactionIsolation());
			}
		};
		
		try
		{
			return ((Integer) this.executeRead(operation)).intValue();
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * @see javax.sql.RowSet#clearParameters()
	 */
	public void clearParameters() throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.clearParameters();
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#execute()
	 */
	public void execute() throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.execute();
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#getEscapeProcessing()
	 */
	public boolean getEscapeProcessing() throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				return new Boolean(rowSet.getEscapeProcessing());
			}
		};
		
		return ((Boolean) this.executeRead(operation)).booleanValue();
	}

	/**
	 * @see javax.sql.RowSet#isReadOnly()
	 */
	public boolean isReadOnly()
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet)
			{
				return new Boolean(rowSet.isReadOnly());
			}
		};
		
		try
		{
			return ((Boolean) this.executeRead(operation)).booleanValue();
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * @see javax.sql.RowSet#setConcurrency(int)
	 */
	public void setConcurrency(final int concurrency) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setConcurrency(concurrency);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setMaxFieldSize(int)
	 */
	public void setMaxFieldSize(final int size) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setMaxFieldSize(size);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setMaxRows(int)
	 */
	public void setMaxRows(final int rows) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setMaxRows(rows);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setQueryTimeout(int)
	 */
	public void setQueryTimeout(final int seconds) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setQueryTimeout(seconds);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setTransactionIsolation(int)
	 */
	public void setTransactionIsolation(final int isolation) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setTransactionIsolation(isolation);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setType(int)
	 */
	public void setType(final int type) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setType(type);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setByte(int, byte)
	 */
	public void setByte(final int index, final byte value) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setByte(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setDouble(int, double)
	 */
	public void setDouble(final int index, final double value) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setDouble(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setFloat(int, float)
	 */
	public void setFloat(final int index, final float value) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setFloat(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setInt(int, int)
	 */
	public void setInt(final int index, final int value) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setInt(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setNull(int, int)
	 */
	public void setNull(final int index, final int sqlType) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setNull(index, sqlType);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setLong(int, long)
	 */
	public void setLong(final int index, final long value) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setLong(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setShort(int, short)
	 */
	public void setShort(final int index, final short value) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setShort(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setBoolean(int, boolean)
	 */
	public void setBoolean(final int index, final boolean value) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setBoolean(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setBytes(int, byte[])
	 */
	public void setBytes(final int index, final byte[] value) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setBytes(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setEscapeProcessing(boolean)
	 */
	public void setEscapeProcessing(final boolean enabled) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setEscapeProcessing(enabled);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setReadOnly(boolean)
	 */
	public void setReadOnly(final boolean enabled) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setReadOnly(enabled);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setAsciiStream(int, java.io.InputStream, int)
	 */
	public void setAsciiStream(final int index, final InputStream inputStream, final int length) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setAsciiStream(index, inputStream, length);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setBinaryStream(int, java.io.InputStream, int)
	 */
	public void setBinaryStream(final int index, final InputStream inputStream, final int length) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setBinaryStream(index, inputStream, length);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setCharacterStream(int, java.io.Reader, int)
	 */
	public void setCharacterStream(final int index, final Reader reader, final int length) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setCharacterStream(index, reader, length);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setObject(int, java.lang.Object)
	 */
	public void setObject(final int index, final Object value) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setObject(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setObject(int, java.lang.Object, int)
	 */
	public void setObject(final int index, final Object value, final int sqlType) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setObject(index, value, sqlType);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setObject(int, java.lang.Object, int, int)
	 */
	public void setObject(final int index, final Object value, final int sqlType, final int scale) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setObject(index, value, sqlType, scale);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#getCommand()
	 */
	public String getCommand()
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet)
			{
				return rowSet.getCommand();
			}
		};
		
		try
		{
			return (String) this.executeRead(operation);
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * @see javax.sql.RowSet#getDataSourceName()
	 */
	public String getDataSourceName()
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet)
			{
				return rowSet.getDataSourceName();
			}
		};
		
		try
		{
			return (String) this.executeRead(operation);
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * @see javax.sql.RowSet#getPassword()
	 */
	public String getPassword()
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet)
			{
				return rowSet.getPassword();
			}
		};
		
		try
		{
			return (String) this.executeRead(operation);
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * @see javax.sql.RowSet#getUrl()
	 */
	public String getUrl() throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				return rowSet.getUrl();
			}
		};
		
		return (String) this.executeRead(operation);
	}

	/**
	 * @see javax.sql.RowSet#getUsername()
	 */
	public String getUsername()
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet)
			{
				return rowSet.getUsername();
			}
		};
		
		try
		{
			return (String) this.executeRead(operation);
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * @see javax.sql.RowSet#setNull(int, int, java.lang.String)
	 */
	public void setNull(final int index, final int sqlType, final String typeName) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setNull(index, sqlType, typeName);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setString(int, java.lang.String)
	 */
	public void setString(final int index, final String value) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setString(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setCommand(java.lang.String)
	 */
	public void setCommand(final String command) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setCommand(command);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setDataSourceName(java.lang.String)
	 */
	public void setDataSourceName(final String name) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setDataSourceName(name);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setPassword(java.lang.String)
	 */
	public void setPassword(final String password) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setPassword(password);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setUrl(java.lang.String)
	 */
	public void setUrl(final String url) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setUrl(url);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setUsername(java.lang.String)
	 */
	public void setUsername(final String username) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setUsername(username);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setBigDecimal(int, java.math.BigDecimal)
	 */
	public void setBigDecimal(final int index, final BigDecimal value) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setBigDecimal(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setArray(int, java.sql.Array)
	 */
	public void setArray(final int index, final Array value) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setArray(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setBlob(int, java.sql.Blob)
	 */
	public void setBlob(final int index, final Blob value) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setBlob(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setClob(int, java.sql.Clob)
	 */
	public void setClob(final int index, final Clob value) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setClob(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setDate(int, java.sql.Date)
	 */
	public void setDate(final int index, final Date value) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setDate(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setRef(int, java.sql.Ref)
	 */
	public void setRef(final int index, final Ref value) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setRef(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setTime(int, java.sql.Time)
	 */
	public void setTime(final int index, final Time value) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setTime(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setTimestamp(int, java.sql.Timestamp)
	 */
	public void setTimestamp(final int index, final Timestamp value) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setTimestamp(index, value);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#getTypeMap()
	 */
	public Map getTypeMap() throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				return rowSet.getTypeMap();
			}
		};
		
		return (Map) this.executeRead(operation);
	}

	/**
	 * @see javax.sql.RowSet#setTypeMap(java.util.Map)
	 */
	public void setTypeMap(final Map typeMap) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setTypeMap(typeMap);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#addRowSetListener(javax.sql.RowSetListener)
	 */
	public void addRowSetListener(final RowSetListener listener)
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet)
			{
				rowSet.addRowSetListener(listener);
				
				return null;
			}
		};
		
		try
		{
			this.executeWrite(operation);
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * @see javax.sql.RowSet#removeRowSetListener(javax.sql.RowSetListener)
	 */
	public void removeRowSetListener(final RowSetListener listener)
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet)
			{
				rowSet.removeRowSetListener(listener);
				
				return null;
			}
		};
		
		try
		{
			this.executeWrite(operation);
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * @see javax.sql.RowSet#setDate(int, java.sql.Date, java.util.Calendar)
	 */
	public void setDate(final int index, final Date value, final Calendar calendar) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setDate(index, value, calendar);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setTime(int, java.sql.Time, java.util.Calendar)
	 */
	public void setTime(final int index, final Time value, final Calendar calendar) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setTime(index, value, calendar);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}

	/**
	 * @see javax.sql.RowSet#setTimestamp(int, java.sql.Timestamp, java.util.Calendar)
	 */
	public void setTimestamp(final int index, final Timestamp value, final Calendar calendar) throws SQLException
	{
		RowSetOperation operation = new RowSetOperation()
		{
			public Object execute(RowSet rowSet) throws SQLException
			{
				rowSet.setTimestamp(index, value, calendar);
				
				return null;
			}
		};
		
		this.executeWrite(operation);
	}
	
	protected abstract static class RowSetOperation extends ResultSetProxy.ResultSetOperation
	{
		public abstract Object execute(RowSet rowSet) throws SQLException;
		
		public final Object execute(ResultSet resultSet) throws SQLException
		{
			return this.execute((RowSet) resultSet);
		}
	}
}
