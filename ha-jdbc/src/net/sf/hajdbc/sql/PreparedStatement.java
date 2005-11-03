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

import java.io.File;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class PreparedStatement extends Statement implements java.sql.PreparedStatement
{
	private String sql;
	
	/**
	 * Constructs a new PreparedStatementProxy.
	 * @param connection a Connection proxy
	 * @param operation an operation that creates PreparedStatements
	 * @param sql an SQL statement
	 * @throws java.sql.SQLException if operation execution fails
	 */
	public PreparedStatement(Connection connection, ConnectionOperation operation, String sql) throws java.sql.SQLException
	{
		super(connection, operation);
		
		this.sql = sql;
	}
	
	/**
	 * @see java.sql.PreparedStatement#addBatch()
	 */
	public void addBatch() throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				statement.addBatch();
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
		
		String object = this.extractMutexObject(this.sql);
		
		if (object != null)
		{
			this.mutexObjectSet.add(object);
		}
	}

	/**
	 * @see java.sql.PreparedStatement#clearParameters()
	 */
	public void clearParameters() throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				statement.clearParameters();
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.PreparedStatement#execute()
	 */
	public boolean execute() throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				return new Boolean(statement.execute());
			}
		};
		
		return ((Boolean) this.firstValue(this.executeWriteToDatabase(operation, this.extractMutexObject(this.sql)))).booleanValue();
	}

	/**
	 * @see java.sql.PreparedStatement#executeQuery()
	 */
	public java.sql.ResultSet executeQuery() throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				return statement.executeQuery();
			}
		};

		String object = this.extractMutexObject(this.sql);
		
		return ((object == null) && (this.getResultSetConcurrency() == java.sql.ResultSet.CONCUR_READ_ONLY) && !this.isSelectForUpdate(this.sql)) ? (java.sql.ResultSet) this.executeReadFromDatabase(operation) : new ResultSet(this, operation, object);
	}

	/**
	 * @see java.sql.PreparedStatement#executeUpdate()
	 */
	public int executeUpdate() throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				return new Integer(statement.executeUpdate());
			}
		};
		
		return ((Integer) this.firstValue(this.executeWriteToDatabase(operation, this.extractMutexObject(this.sql)))).intValue();
	}

	/**
	 * @see java.sql.PreparedStatement#getMetaData()
	 */
	public ResultSetMetaData getMetaData() throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				return statement.getMetaData();
			}
		};
		
		return (ResultSetMetaData) this.executeReadFromDatabase(operation);
	}

	/**
	 * @see java.sql.PreparedStatement#getParameterMetaData()
	 */
	public ParameterMetaData getParameterMetaData() throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				return statement.getParameterMetaData();
			}
		};
		
		return (ParameterMetaData) this.executeReadFromDatabase(operation);
	}

	/**
	 * @see java.sql.PreparedStatement#setArray(int, java.sql.Array)
	 */
	public void setArray(final int index, final Array value) throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				statement.setArray(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.PreparedStatement#setAsciiStream(int, java.io.InputStream, int)
	 */
	public void setAsciiStream(final int index, InputStream inputStream, final int length) throws SQLException
	{
		final FileSupport fileSupport = this.getFileSupport();
		final File file = fileSupport.createFile(inputStream);
		
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				statement.setAsciiStream(index, fileSupport.getInputStream(file), length);
			
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.PreparedStatement#setBigDecimal(int, java.math.BigDecimal)
	 */
	public void setBigDecimal(final int index, final BigDecimal value) throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				statement.setBigDecimal(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.PreparedStatement#setBinaryStream(int, java.io.InputStream, int)
	 */
	public void setBinaryStream(final int index, InputStream inputStream, final int length) throws SQLException
	{
		final FileSupport fileSupport = this.getFileSupport();
		final File file = fileSupport.createFile(inputStream);

		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				statement.setBinaryStream(index, fileSupport.getInputStream(file), length);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);			
	}

	/**
	 * @see java.sql.PreparedStatement#setBlob(int, java.sql.Blob)
	 */
	public void setBlob(final int index, final java.sql.Blob value) throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				statement.setBlob(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.PreparedStatement#setBoolean(int, boolean)
	 */
	public void setBoolean(final int index, final boolean value) throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				statement.setBoolean(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.PreparedStatement#setByte(int, byte)
	 */
	public void setByte(final int index, final byte value) throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				statement.setByte(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.PreparedStatement#setBytes(int, byte[])
	 */
	public void setBytes(final int index, final byte[] value) throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				statement.setBytes(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.PreparedStatement#setCharacterStream(int, java.io.Reader, int)
	 */
	public void setCharacterStream(final int index, Reader reader, final int length) throws SQLException
	{
		final FileSupport fileSupport = this.getFileSupport();
		final File file = fileSupport.createFile(reader);
		
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				statement.setCharacterStream(index, fileSupport.getReader(file), length);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}
	
	/**
	 * @see java.sql.PreparedStatement#setClob(int, java.sql.Clob)
	 */
	public void setClob(final int index, final java.sql.Clob value) throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				statement.setClob(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.PreparedStatement#setDate(int, java.sql.Date)
	 */
	public void setDate(final int index, final Date value) throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				statement.setDate(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.PreparedStatement#setDate(int, java.sql.Date, java.util.Calendar)
	 */
	public void setDate(final int index, final Date value, final Calendar calendar) throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				statement.setDate(index, value, calendar);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.PreparedStatement#setDouble(int, double)
	 */
	public void setDouble(final int index, final double value) throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				statement.setDouble(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.PreparedStatement#setFloat(int, float)
	 */
	public void setFloat(final int index, final float value) throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				statement.setFloat(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.PreparedStatement#setInt(int, int)
	 */
	public void setInt(final int index, final int value) throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				statement.setInt(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.PreparedStatement#setLong(int, long)
	 */
	public void setLong(final int index, final long value) throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				statement.setLong(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.PreparedStatement#setNull(int, int)
	 */
	public void setNull(final int index, final int sqlType) throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				statement.setNull(index, sqlType);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.PreparedStatement#setNull(int, int, java.lang.String)
	 */
	public void setNull(final int index, final int sqlType, final String typeName) throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				statement.setNull(index, sqlType, typeName);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.PreparedStatement#setObject(int, java.lang.Object)
	 */
	public void setObject(final int index, final Object value) throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				statement.setObject(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.PreparedStatement#setObject(int, java.lang.Object, int)
	 */
	public void setObject(final int index, final Object value, final int sqlType) throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				statement.setObject(index, value, sqlType);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.PreparedStatement#setObject(int, java.lang.Object, int, int)
	 */
	public void setObject(final int index, final Object value, final int sqlType, final int scale) throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				statement.setObject(index, value, sqlType, scale);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.PreparedStatement#setRef(int, java.sql.Ref)
	 */
	public void setRef(final int index, final Ref value) throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				statement.setRef(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.PreparedStatement#setShort(int, short)
	 */
	public void setShort(final int index, final short value) throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				statement.setShort(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.PreparedStatement#setString(int, java.lang.String)
	 */
	public void setString(final int index, final String value) throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				statement.setString(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.PreparedStatement#setTime(int, java.sql.Time)
	 */
	public void setTime(final int index, final Time value) throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				statement.setTime(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.PreparedStatement#setTime(int, java.sql.Time, java.util.Calendar)
	 */
	public void setTime(final int index, final Time value, final Calendar calendar) throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				statement.setTime(index, value, calendar);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.PreparedStatement#setTimestamp(int, java.sql.Timestamp)
	 */
	public void setTimestamp(final int index, final Timestamp value) throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				statement.setTimestamp(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.PreparedStatement#setTimestamp(int, java.sql.Timestamp, java.util.Calendar)
	 */
	public void setTimestamp(final int index, final Timestamp value, final Calendar calendar) throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				statement.setTimestamp(index, value, calendar);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.PreparedStatement#setUnicodeStream(int, java.io.InputStream, int)
	 * @deprecated
	 */
	public void setUnicodeStream(final int index, InputStream inputStream, final int length) throws SQLException
	{
		final FileSupport fileSupport = this.getFileSupport();
		final File file = fileSupport.createFile(inputStream);

		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				statement.setUnicodeStream(index, fileSupport.getInputStream(file), length);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}
	
	/**
	 * @see java.sql.PreparedStatement#setURL(int, java.net.URL)
	 */
	public void setURL(final int index, final URL value) throws SQLException
	{
		PreparedStatementOperation operation = new PreparedStatementOperation()
		{
			public Object execute(java.sql.PreparedStatement statement) throws SQLException
			{
				statement.setURL(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}
	
	protected FileSupport getFileSupport() throws SQLException
	{
		Connection connection = (Connection) this.getConnection();
		
		return connection.getFileSupport();
	}
}
