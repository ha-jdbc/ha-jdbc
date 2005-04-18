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
import java.sql.Ref;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class CallableStatement extends PreparedStatement implements java.sql.CallableStatement
{
	/**
	 * Constructs a new CallableStatementProxy.
	 * @param connection a Connection proxy
	 * @param operation an operation that creates CallableStatements
	 * @throws java.sql.SQLException if operation execution fails
	 */
	public CallableStatement(Connection connection, ConnectionOperation operation) throws java.sql.SQLException
	{
		super(connection, operation);
	}
	
	/**
	 * @see java.sql.CallableStatement#getArray(int)
	 */
	public Array getArray(final int index) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getArray(index);
			}
		};
		
		return (Array) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#getArray(java.lang.String)
	 */
	public Array getArray(final String name) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getArray(name);
			}
		};
		
		return (Array) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#getBigDecimal(int)
	 */
	public BigDecimal getBigDecimal(final int index) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getBigDecimal(index);
			}
		};
		
		return (BigDecimal) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#getBigDecimal(int, int)
	 * @deprecated
	 */
	public BigDecimal getBigDecimal(final int index, final int scale) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getBigDecimal(index, scale);
			}
		};
		
		return (BigDecimal) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#getBigDecimal(java.lang.String)
	 */
	public BigDecimal getBigDecimal(final String name) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getBigDecimal(name);
			}
		};
		
		return (BigDecimal) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#getBlob(int)
	 */
	public java.sql.Blob getBlob(final int index) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getBlob(index);
			}
		};
		
		return (java.sql.Blob) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#getBlob(java.lang.String)
	 */
	public java.sql.Blob getBlob(final String name) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getBlob(name);
			}
		};
		
		return (java.sql.Blob) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#getBoolean(int)
	 */
	public boolean getBoolean(final int index) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return new Boolean(statement.getBoolean(index));
			}
		};
		
		return ((Boolean) this.executeReadFromDriver(operation)).booleanValue();
	}

	/**
	 * @see java.sql.CallableStatement#getBoolean(java.lang.String)
	 */
	public boolean getBoolean(final String name) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return new Boolean(statement.getBoolean(name));
			}
		};
		
		return ((Boolean) this.executeReadFromDriver(operation)).booleanValue();
	}

	/**
	 * @see java.sql.CallableStatement#getByte(int)
	 */
	public byte getByte(final int index) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return new Byte(statement.getByte(index));
			}
		};
		
		return ((Byte) this.executeReadFromDriver(operation)).byteValue();
	}

	/**
	 * @see java.sql.CallableStatement#getByte(java.lang.String)
	 */
	public byte getByte(final String name) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return new Byte(statement.getByte(name));
			}
		};
		
		return ((Byte) this.executeReadFromDriver(operation)).byteValue();
	}

	/**
	 * @see java.sql.CallableStatement#getBytes(int)
	 */
	public byte[] getBytes(final int index) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getBytes(index);
			}
		};
		
		return (byte[]) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#getBytes(java.lang.String)
	 */
	public byte[] getBytes(final String name) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getBytes(name);
			}
		};
		
		return (byte[]) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#getClob(int)
	 */
	public java.sql.Clob getClob(final int index) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getClob(index);
			}
		};
		
		return (java.sql.Clob) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#getClob(java.lang.String)
	 */
	public java.sql.Clob getClob(final String name) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getClob(name);
			}
		};
		
		return (java.sql.Clob) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#getDate(int)
	 */
	public Date getDate(final int index) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getDate(index);
			}
		};
		
		return (Date) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#getDate(int, java.util.Calendar)
	 */
	public Date getDate(final int index, final Calendar calendar) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getDate(index, calendar);
			}
		};
		
		return (Date) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#getDate(java.lang.String)
	 */
	public Date getDate(final String name) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getDate(name);
			}
		};
		
		return (Date) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#getDate(java.lang.String, java.util.Calendar)
	 */
	public Date getDate(final String name, final Calendar calendar) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getDate(name, calendar);
			}
		};
		
		return (Date) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#getDouble(int)
	 */
	public double getDouble(final int index) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return new Double(statement.getDouble(index));
			}
		};
		
		return ((Double) this.executeReadFromDriver(operation)).doubleValue();
	}

	/**
	 * @see java.sql.CallableStatement#getDouble(java.lang.String)
	 */
	public double getDouble(final String name) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return new Double(statement.getDouble(name));
			}
		};
		
		return ((Double) this.executeReadFromDriver(operation)).doubleValue();
	}

	/**
	 * @see java.sql.CallableStatement#getFloat(int)
	 */
	public float getFloat(final int index) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return new Float(statement.getFloat(index));
			}
		};
		
		return ((Float) this.executeReadFromDriver(operation)).floatValue();
	}

	/**
	 * @see java.sql.CallableStatement#getFloat(java.lang.String)
	 */
	public float getFloat(final String name) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return new Float(statement.getFloat(name));
			}
		};
		
		return ((Float) this.executeReadFromDriver(operation)).floatValue();
	}

	/**
	 * @see java.sql.CallableStatement#getInt(int)
	 */
	public int getInt(final int index) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return new Integer(statement.getInt(index));
			}
		};
		
		return ((Integer) this.executeReadFromDriver(operation)).intValue();
	}

	/**
	 * @see java.sql.CallableStatement#getInt(java.lang.String)
	 */
	public int getInt(final String name) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return new Integer(statement.getInt(name));
			}
		};
		
		return ((Integer) this.executeReadFromDriver(operation)).intValue();
	}

	/**
	 * @see java.sql.CallableStatement#getLong(int)
	 */
	public long getLong(final int index) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return new Long(statement.getLong(index));
			}
		};
		
		return ((Long) this.executeReadFromDriver(operation)).longValue();
	}

	/**
	 * @see java.sql.CallableStatement#getLong(java.lang.String)
	 */
	public long getLong(final String name) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return new Long(statement.getLong(name));
			}
		};
		
		return ((Long) this.executeReadFromDriver(operation)).longValue();
	}

	/**
	 * @see java.sql.CallableStatement#getObject(int)
	 */
	public Object getObject(final int index) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getObject(index);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#getObject(int, java.util.Map)
	 */
	public Object getObject(final int index, final Map typeMap) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getObject(index, typeMap);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#getObject(java.lang.String)
	 */
	public Object getObject(final String name) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getObject(name);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#getObject(java.lang.String, java.util.Map)
	 */
	public Object getObject(final String name, final Map typeMap) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getObject(name, typeMap);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#getRef(int)
	 */
	public Ref getRef(final int index) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getRef(index);
			}
		};
		
		return (Ref) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#getRef(java.lang.String)
	 */
	public Ref getRef(final String name) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getRef(name);
			}
		};
		
		return (Ref) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#getShort(int)
	 */
	public short getShort(final int index) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return new Short(statement.getShort(index));
			}
		};
		
		return ((Short) this.executeReadFromDriver(operation)).shortValue();
	}

	/**
	 * @see java.sql.CallableStatement#getShort(java.lang.String)
	 */
	public short getShort(final String name) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return new Short(statement.getShort(name));
			}
		};
		
		return ((Short) this.executeReadFromDriver(operation)).shortValue();
	}

	/**
	 * @see java.sql.CallableStatement#getString(int)
	 */
	public String getString(final int index) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getString(index);
			}
		};
		
		return (String) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#getString(java.lang.String)
	 */
	public String getString(final String name) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getString(name);
			}
		};
		
		return (String) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#getTime(int)
	 */
	public Time getTime(final int index) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getTime(index);
			}
		};
		
		return (Time) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#getTime(int, java.util.Calendar)
	 */
	public Time getTime(final int index, final Calendar calendar) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getTime(index, calendar);
			}
		};
		
		return (Time) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#getTime(java.lang.String)
	 */
	public Time getTime(final String name) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getTime(name);
			}
		};
		
		return (Time) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#getTime(java.lang.String, java.util.Calendar)
	 */
	public Time getTime(final String name, final Calendar calendar) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getTime(name, calendar);
			}
		};
		
		return (Time) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#getTimestamp(int)
	 */
	public Timestamp getTimestamp(final int index) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getTimestamp(index);
			}
		};
		
		return (Timestamp) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#getTimestamp(int, java.util.Calendar)
	 */
	public Timestamp getTimestamp(final int index, final Calendar calendar) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getTimestamp(index, calendar);
			}
		};
		
		return (Timestamp) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#getTimestamp(java.lang.String)
	 */
	public Timestamp getTimestamp(final String name) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getTimestamp(name);
			}
		};
		
		return (Timestamp) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#getTimestamp(java.lang.String, java.util.Calendar)
	 */
	public Timestamp getTimestamp(final String name, final Calendar calendar) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getTimestamp(name, calendar);
			}
		};
		
		return (Timestamp) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#getURL(int)
	 */
	public URL getURL(final int index) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getURL(index);
			}
		};
		
		return (URL) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#getURL(java.lang.String)
	 */
	public URL getURL(final String name) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return statement.getURL(name);
			}
		};
		
		return (URL) this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#registerOutParameter(int, int)
	 */
	public void registerOutParameter(final int index, final int sqlType) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				statement.registerOutParameter(index, sqlType);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#registerOutParameter(int, int, int)
	 */
	public void registerOutParameter(final int index, final int sqlType, final int scale) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				statement.registerOutParameter(index, sqlType, scale);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#registerOutParameter(int, int, java.lang.String)
	 */
	public void registerOutParameter(final int index, final int sqlType, final String typeName) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				statement.registerOutParameter(index, sqlType, typeName);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#registerOutParameter(java.lang.String, int)
	 */
	public void registerOutParameter(final String name, final int sqlType) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				statement.registerOutParameter(name, sqlType);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#registerOutParameter(java.lang.String, int, int)
	 */
	public void registerOutParameter(final String name, final int sqlType, final int scale) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				statement.registerOutParameter(name, sqlType, scale);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#registerOutParameter(java.lang.String, int, java.lang.String)
	 */
	public void registerOutParameter(final String name, final int sqlType, final String typeName) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				statement.registerOutParameter(name, sqlType, typeName);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#setAsciiStream(java.lang.String, java.io.InputStream, int)
	 */
	public void setAsciiStream(final String name, InputStream inputStream, final int length) throws SQLException
	{
		final File file = this.fileSupport.createFile(inputStream);
		
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				statement.setAsciiStream(name, CallableStatement.this.fileSupport.getInputStream(file), length);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#setBigDecimal(java.lang.String, java.math.BigDecimal)
	 */
	public void setBigDecimal(final String name, final BigDecimal value) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				statement.setBigDecimal(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#setBinaryStream(java.lang.String, java.io.InputStream, int)
	 */
	public void setBinaryStream(final String name, InputStream inputStream, final int length) throws SQLException
	{
		final File file = this.fileSupport.createFile(inputStream);
		
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				statement.setBinaryStream(name, CallableStatement.this.fileSupport.getInputStream(file), length);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#setBoolean(java.lang.String, boolean)
	 */
	public void setBoolean(final String name, final boolean value) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				statement.setBoolean(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#setByte(java.lang.String, byte)
	 */
	public void setByte(final String name, final byte value) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				statement.setByte(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#setBytes(java.lang.String, byte[])
	 */
	public void setBytes(final String name, final byte[] value) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				statement.setBytes(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#setCharacterStream(java.lang.String, java.io.Reader, int)
	 */
	public void setCharacterStream(final String name, Reader reader, final int length) throws SQLException
	{
		final File file = this.fileSupport.createFile(reader);
		
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				statement.setCharacterStream(name, CallableStatement.this.fileSupport.getReader(file), length);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#setDate(java.lang.String, java.sql.Date)
	 */
	public void setDate(final String name, final Date value) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				statement.setDate(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#setDate(java.lang.String, java.sql.Date, java.util.Calendar)
	 */
	public void setDate(final String name, final Date value, final Calendar calendar) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				statement.setDate(name, value, calendar);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#setDouble(java.lang.String, double)
	 */
	public void setDouble(final String name, final double value) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				statement.setDouble(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#setFloat(java.lang.String, float)
	 */
	public void setFloat(final String name, final float value) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				statement.setFloat(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#setInt(java.lang.String, int)
	 */
	public void setInt(final String name, final int value) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				statement.setInt(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#setLong(java.lang.String, long)
	 */
	public void setLong(final String name, final long value) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				statement.setLong(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#setNull(java.lang.String, int)
	 */
	public void setNull(final String name, final int sqlType) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				statement.setNull(name, sqlType);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#setNull(java.lang.String, int, java.lang.String)
	 */
	public void setNull(final String name, final int sqlType, final String typeName) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				statement.setNull(name, sqlType, typeName);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#setObject(java.lang.String, java.lang.Object)
	 */
	public void setObject(final String name, final Object value) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				statement.setObject(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#setObject(java.lang.String, java.lang.Object, int)
	 */
	public void setObject(final String name, final Object value, final int sqlType) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				statement.setObject(name, value, sqlType);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#setObject(java.lang.String, java.lang.Object, int, int)
	 */
	public void setObject(final String name, final Object value, final int sqlType, final int scale) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				statement.setObject(name, value, sqlType, scale);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#setShort(java.lang.String, short)
	 */
	public void setShort(final String name, final short value) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				statement.setShort(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#setString(java.lang.String, java.lang.String)
	 */
	public void setString(final String name, final String value) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				statement.setString(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#setTime(java.lang.String, java.sql.Time)
	 */
	public void setTime(final String name, final Time value) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				statement.setTime(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#setTime(java.lang.String, java.sql.Time, java.util.Calendar)
	 */
	public void setTime(final String name, final Time value, final Calendar calendar) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				statement.setTime(name, value, calendar);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#setTimestamp(java.lang.String, java.sql.Timestamp)
	 */
	public void setTimestamp(final String name, final Timestamp value) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				statement.setTimestamp(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#setTimestamp(java.lang.String, java.sql.Timestamp, java.util.Calendar)
	 */
	public void setTimestamp(final String name, final Timestamp value, final Calendar calendar) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				statement.setTimestamp(name, value, calendar);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#setURL(java.lang.String, java.net.URL)
	 */
	public void setURL(final String name, final URL value) throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				statement.setURL(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.CallableStatement#wasNull()
	 */
	public boolean wasNull() throws SQLException
	{
		CallableStatementOperation operation = new CallableStatementOperation()
		{
			public Object execute(java.sql.CallableStatement statement) throws SQLException
			{
				return new Boolean(statement.wasNull());
			}
		};
		
		return ((Boolean) this.executeReadFromDriver(operation)).booleanValue();
	}
}
