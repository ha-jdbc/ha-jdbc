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
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.Operation;
import net.sf.hajdbc.SQLObject;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @param <T> 
 * @since   1.0
 */
public class ResultSet<T extends java.sql.Statement> extends SQLObject<java.sql.ResultSet, T> implements java.sql.ResultSet
{
	/**
	 * Constructs a new ResultSetProxy.
	 * @param statement a Statement proxy
	 * @param operation an operation that creates ResultSets
	 * @throws SQLException if operation execution fails
	 */
	public ResultSet(Statement<T> statement, Operation<T, java.sql.ResultSet> operation) throws SQLException
	{
		super(statement, operation);
	}
	
	/**
	 * @see net.sf.hajdbc.SQLObject#handleExceptions(java.util.Map)
	 */
	public void handleExceptions(Map exceptionMap) throws SQLException
	{
		this.parent.handleExceptions(exceptionMap);
	}

	/**
	 * @see java.sql.ResultSet#absolute(int)
	 */
	public boolean absolute(final int index) throws SQLException
	{
		Operation<java.sql.ResultSet, Boolean> operation = new Operation<java.sql.ResultSet, Boolean>()
		{
			public Boolean execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.absolute(index);
			}
		};
		
		return this.firstValue(this.executeWriteToDriver(operation));
	}

	/**
	 * @see java.sql.ResultSet#afterLast()
	 */
	public void afterLast() throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.afterLast();
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#beforeFirst()
	 */
	public void beforeFirst() throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.beforeFirst();
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#cancelRowUpdates()
	 */
	public void cancelRowUpdates() throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.cancelRowUpdates();
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#clearWarnings()
	 */
	public void clearWarnings() throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.clearWarnings();
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#close()
	 */
	public void close() throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.close();
				
				return null;
			}
		};
		
		this.executeWriteToDatabase(operation);
	}

	/**
	 * @see java.sql.ResultSet#deleteRow()
	 */
	public void deleteRow() throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.deleteRow();
				
				return null;
			}
		};
		
		this.executeWriteToDatabase(operation);
	}

	/**
	 * @see java.sql.ResultSet#findColumn(java.lang.String)
	 */
	public int findColumn(final String name) throws SQLException
	{
		Operation<java.sql.ResultSet, Integer> operation = new Operation<java.sql.ResultSet, Integer>()
		{
			public Integer execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.findColumn(name);
			}
		};

		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#first()
	 */
	public boolean first() throws SQLException
	{
		Operation<java.sql.ResultSet, Boolean> operation = new Operation<java.sql.ResultSet, Boolean>()
		{
			public Boolean execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.first();
			}
		};
		
		return this.firstValue((this.getType() == java.sql.ResultSet.TYPE_SCROLL_SENSITIVE) ? this.executeWriteToDatabase(operation) : this.executeWriteToDriver(operation));
	}

	/**
	 * @see java.sql.ResultSet#getArray(int)
	 */
	public Array getArray(final int index) throws SQLException
	{
		Operation<java.sql.ResultSet, Array> operation = new Operation<java.sql.ResultSet, Array>()
		{
			public Array execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getArray(index);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getArray(java.lang.String)
	 */
	public Array getArray(final String name) throws SQLException
	{
		Operation<java.sql.ResultSet, Array> operation = new Operation<java.sql.ResultSet, Array>()
		{
			public Array execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getArray(name);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getAsciiStream(int)
	 */
	public InputStream getAsciiStream(final int index) throws SQLException
	{
		Operation<java.sql.ResultSet, InputStream> operation = new Operation<java.sql.ResultSet, InputStream>()
		{
			public InputStream execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getAsciiStream(index);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getAsciiStream(java.lang.String)
	 */
	public InputStream getAsciiStream(final String name) throws SQLException
	{
		Operation<java.sql.ResultSet, InputStream> operation = new Operation<java.sql.ResultSet, InputStream>()
		{
			public InputStream execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getAsciiStream(name);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getBigDecimal(int)
	 */
	public BigDecimal getBigDecimal(final int index) throws SQLException
	{
		Operation<java.sql.ResultSet, BigDecimal> operation = new Operation<java.sql.ResultSet, BigDecimal>()
		{
			public BigDecimal execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getBigDecimal(index);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getBigDecimal(int, int)
	 * @deprecated
	 */
	public BigDecimal getBigDecimal(final int index, final int scale) throws SQLException
	{
		Operation<java.sql.ResultSet, BigDecimal> operation = new Operation<java.sql.ResultSet, BigDecimal>()
		{
			public BigDecimal execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getBigDecimal(index, scale);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getBigDecimal(java.lang.String)
	 */
	public BigDecimal getBigDecimal(final String name) throws SQLException
	{
		Operation<java.sql.ResultSet, BigDecimal> operation = new Operation<java.sql.ResultSet, BigDecimal>()
		{
			public BigDecimal execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getBigDecimal(name);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getBigDecimal(java.lang.String, int)
	 * @deprecated
	 */
	public BigDecimal getBigDecimal(final String name, final int scale) throws SQLException
	{
		Operation<java.sql.ResultSet, BigDecimal> operation = new Operation<java.sql.ResultSet, BigDecimal>()
		{
			public BigDecimal execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getBigDecimal(name, scale);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getBinaryStream(int)
	 */
	public InputStream getBinaryStream(final int index) throws SQLException
	{
		Operation<java.sql.ResultSet, InputStream> operation = new Operation<java.sql.ResultSet, InputStream>()
		{
			public InputStream execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getBinaryStream(index);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getBinaryStream(java.lang.String)
	 */
	public InputStream getBinaryStream(final String name) throws SQLException
	{
		Operation<java.sql.ResultSet, InputStream> operation = new Operation<java.sql.ResultSet, InputStream>()
		{
			public InputStream execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getBinaryStream(name);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getBlob(int)
	 */
	public Blob getBlob(final int index) throws SQLException
	{
		Operation<java.sql.ResultSet, java.sql.Blob> operation = new Operation<java.sql.ResultSet, Blob>()
		{
			public Blob execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getBlob(index);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getBlob(java.lang.String)
	 */
	public Blob getBlob(final String name) throws SQLException
	{
		Operation<java.sql.ResultSet, java.sql.Blob> operation = new Operation<java.sql.ResultSet, Blob>()
		{
			public Blob execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getBlob(name);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getBoolean(int)
	 */
	public boolean getBoolean(final int index) throws SQLException
	{
		Operation<java.sql.ResultSet, Boolean> operation = new Operation<java.sql.ResultSet, Boolean>()
		{
			public Boolean execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getBoolean(index);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getBoolean(java.lang.String)
	 */
	public boolean getBoolean(final String name) throws SQLException
	{
		Operation<java.sql.ResultSet, Boolean> operation = new Operation<java.sql.ResultSet, Boolean>()
		{
			public Boolean execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getBoolean(name);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getByte(int)
	 */
	public byte getByte(final int index) throws SQLException
	{
		Operation<java.sql.ResultSet, Byte> operation = new Operation<java.sql.ResultSet, Byte>()
		{
			public Byte execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getByte(index);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getByte(java.lang.String)
	 */
	public byte getByte(final String name) throws SQLException
	{
		Operation<java.sql.ResultSet, Byte> operation = new Operation<java.sql.ResultSet, Byte>()
		{
			public Byte execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getByte(name);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getBytes(int)
	 */
	public byte[] getBytes(final int index) throws SQLException
	{
		Operation<java.sql.ResultSet, byte[]> operation = new Operation<java.sql.ResultSet, byte[]>()
		{
			public byte[] execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getBytes(index);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getBytes(java.lang.String)
	 */
	public byte[] getBytes(final String name) throws SQLException
	{
		Operation<java.sql.ResultSet, byte[]> operation = new Operation<java.sql.ResultSet, byte[]>()
		{
			public byte[] execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getBytes(name);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getCharacterStream(int)
	 */
	public Reader getCharacterStream(final int index) throws SQLException
	{
		Operation<java.sql.ResultSet, Reader> operation = new Operation<java.sql.ResultSet, Reader>()
		{
			public Reader execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getCharacterStream(index);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getCharacterStream(java.lang.String)
	 */
	public Reader getCharacterStream(final String name) throws SQLException
	{
		Operation<java.sql.ResultSet, Reader> operation = new Operation<java.sql.ResultSet, Reader>()
		{
			public Reader execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getCharacterStream(name);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getClob(int)
	 */
	public Clob getClob(final int index) throws SQLException
	{
		Operation<java.sql.ResultSet, Clob> operation = new Operation<java.sql.ResultSet, Clob>()
		{
			public Clob execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getClob(index);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getClob(java.lang.String)
	 */
	public Clob getClob(final String name) throws SQLException
	{
		Operation<java.sql.ResultSet, Clob> operation = new Operation<java.sql.ResultSet, Clob>()
		{
			public Clob execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getClob(name);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getConcurrency()
	 */
	public int getConcurrency() throws SQLException
	{
		Operation<java.sql.ResultSet, Integer> operation = new Operation<java.sql.ResultSet, Integer>()
		{
			public Integer execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getConcurrency();
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getCursorName()
	 */
	public String getCursorName() throws SQLException
	{
		Operation<java.sql.ResultSet, String> operation = new Operation<java.sql.ResultSet, String>()
		{
			public String execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getCursorName();
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getDate(int)
	 */
	public Date getDate(final int index) throws SQLException
	{
		Operation<java.sql.ResultSet, Date> operation = new Operation<java.sql.ResultSet, Date>()
		{
			public Date execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getDate(index);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getDate(int, java.util.Calendar)
	 */
	public Date getDate(final int index, final Calendar calendar) throws SQLException
	{
		Operation<java.sql.ResultSet, Date> operation = new Operation<java.sql.ResultSet, Date>()
		{
			public Date execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getDate(index, calendar);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getDate(java.lang.String)
	 */
	public Date getDate(final String name) throws SQLException
	{
		Operation<java.sql.ResultSet, Date> operation = new Operation<java.sql.ResultSet, Date>()
		{
			public Date execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getDate(name);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getDate(java.lang.String, java.util.Calendar)
	 */
	public Date getDate(final String name, final Calendar calendar) throws SQLException
	{
		Operation<java.sql.ResultSet, Date> operation = new Operation<java.sql.ResultSet, Date>()
		{
			public Date execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getDate(name, calendar);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getDouble(int)
	 */
	public double getDouble(final int index) throws SQLException
	{
		Operation<java.sql.ResultSet, Double> operation = new Operation<java.sql.ResultSet, Double>()
		{
			public Double execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getDouble(index);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getDouble(java.lang.String)
	 */
	public double getDouble(final String name) throws SQLException
	{
		Operation<java.sql.ResultSet, Double> operation = new Operation<java.sql.ResultSet, Double>()
		{
			public Double execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getDouble(name);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getFetchDirection()
	 */
	public int getFetchDirection() throws SQLException
	{
		Operation<java.sql.ResultSet, Integer> operation = new Operation<java.sql.ResultSet, Integer>()
		{
			public Integer execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getFetchDirection();
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getFetchSize()
	 */
	public int getFetchSize() throws SQLException
	{
		Operation<java.sql.ResultSet, Integer> operation = new Operation<java.sql.ResultSet, Integer>()
		{
			public Integer execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getFetchSize();
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getFloat(int)
	 */
	public float getFloat(final int index) throws SQLException
	{
		Operation<java.sql.ResultSet, Float> operation = new Operation<java.sql.ResultSet, Float>()
		{
			public Float execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getFloat(index);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getFloat(java.lang.String)
	 */
	public float getFloat(final String name) throws SQLException
	{
		Operation<java.sql.ResultSet, Float> operation = new Operation<java.sql.ResultSet, Float>()
		{
			public Float execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getFloat(name);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getInt(int)
	 */
	public int getInt(final int index) throws SQLException
	{
		Operation<java.sql.ResultSet, Integer> operation = new Operation<java.sql.ResultSet, Integer>()
		{
			public Integer execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getInt(index);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getInt(java.lang.String)
	 */
	public int getInt(final String name) throws SQLException
	{
		Operation<java.sql.ResultSet, Integer> operation = new Operation<java.sql.ResultSet, Integer>()
		{
			public Integer execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getInt(name);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getLong(int)
	 */
	public long getLong(final int index) throws SQLException
	{
		Operation<java.sql.ResultSet, Long> operation = new Operation<java.sql.ResultSet, Long>()
		{
			public Long execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getLong(index);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getLong(java.lang.String)
	 */
	public long getLong(final String name) throws SQLException
	{
		Operation<java.sql.ResultSet, Long> operation = new Operation<java.sql.ResultSet, Long>()
		{
			public Long execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getLong(name);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getMetaData()
	 */
	public ResultSetMetaData getMetaData() throws SQLException
	{
		Operation<java.sql.ResultSet, ResultSetMetaData> operation = new Operation<java.sql.ResultSet, ResultSetMetaData>()
		{
			public ResultSetMetaData execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getMetaData();
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getObject(int)
	 */
	public Object getObject(final int index) throws SQLException
	{
		Operation<java.sql.ResultSet, Object> operation = new Operation<java.sql.ResultSet, Object>()
		{
			public Object execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getObject(index);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getObject(int, java.util.Map)
	 */
	public Object getObject(final int index, final Map typeMap) throws SQLException
	{
		Operation<java.sql.ResultSet, Object> operation = new Operation<java.sql.ResultSet, Object>()
		{
			public Object execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getObject(index, typeMap);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getObject(java.lang.String)
	 */
	public Object getObject(final String name) throws SQLException
	{
		Operation<java.sql.ResultSet, Object> operation = new Operation<java.sql.ResultSet, Object>()
		{
			public Object execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getObject(name);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getObject(java.lang.String, java.util.Map)
	 */
	public Object getObject(final String name, final Map typeMap) throws SQLException
	{
		Operation<java.sql.ResultSet, Object> operation = new Operation<java.sql.ResultSet, Object>()
		{
			public Object execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getObject(name, typeMap);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getRef(int)
	 */
	public Ref getRef(final int index) throws SQLException
	{
		Operation<java.sql.ResultSet, Ref> operation = new Operation<java.sql.ResultSet, Ref>()
		{
			public Ref execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getRef(index);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getRef(java.lang.String)
	 */
	public Ref getRef(final String name) throws SQLException
	{
		Operation<java.sql.ResultSet, Ref> operation = new Operation<java.sql.ResultSet, Ref>()
		{
			public Ref execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getRef(name);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getRow()
	 */
	public int getRow() throws SQLException
	{
		Operation<java.sql.ResultSet, Integer> operation = new Operation<java.sql.ResultSet, Integer>()
		{
			public Integer execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getRow();
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getShort(int)
	 */
	public short getShort(final int index) throws SQLException
	{
		Operation<java.sql.ResultSet, Short> operation = new Operation<java.sql.ResultSet, Short>()
		{
			public Short execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getShort(index);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getShort(java.lang.String)
	 */
	public short getShort(final String name) throws SQLException
	{
		Operation<java.sql.ResultSet, Short> operation = new Operation<java.sql.ResultSet, Short>()
		{
			public Short execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getShort(name);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getStatement()
	 */
	public java.sql.Statement getStatement()
	{
		return (Statement) this.parent;
	}

	/**
	 * @see java.sql.ResultSet#getString(int)
	 */
	public String getString(final int index) throws SQLException
	{
		Operation<java.sql.ResultSet, String> operation = new Operation<java.sql.ResultSet, String>()
		{
			public String execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getString(index);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getString(java.lang.String)
	 */
	public String getString(final String name) throws SQLException
	{
		Operation<java.sql.ResultSet, String> operation = new Operation<java.sql.ResultSet, String>()
		{
			public String execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getString(name);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getTime(int)
	 */
	public Time getTime(final int index) throws SQLException
	{
		Operation<java.sql.ResultSet, Time> operation = new Operation<java.sql.ResultSet, Time>()
		{
			public Time execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getTime(index);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getTime(int, java.util.Calendar)
	 */
	public Time getTime(final int index, final Calendar calendar) throws SQLException
	{
		Operation<java.sql.ResultSet, Time> operation = new Operation<java.sql.ResultSet, Time>()
		{
			public Time execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getTime(index, calendar);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getTime(java.lang.String)
	 */
	public Time getTime(final String name) throws SQLException
	{
		Operation<java.sql.ResultSet, Time> operation = new Operation<java.sql.ResultSet, Time>()
		{
			public Time execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getTime(name);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getTime(java.lang.String, java.util.Calendar)
	 */
	public Time getTime(final String name, final Calendar calendar) throws SQLException
	{
		Operation<java.sql.ResultSet, Time> operation = new Operation<java.sql.ResultSet, Time>()
		{
			public Time execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getTime(name, calendar);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getTimestamp(int)
	 */
	public Timestamp getTimestamp(final int index) throws SQLException
	{
		Operation<java.sql.ResultSet, Timestamp> operation = new Operation<java.sql.ResultSet, Timestamp>()
		{
			public Timestamp execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getTimestamp(index);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getTimestamp(int, java.util.Calendar)
	 */
	public Timestamp getTimestamp(final int index, final Calendar calendar) throws SQLException
	{
		Operation<java.sql.ResultSet, Timestamp> operation = new Operation<java.sql.ResultSet, Timestamp>()
		{
			public Timestamp execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getTimestamp(index, calendar);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getTimestamp(java.lang.String)
	 */
	public Timestamp getTimestamp(final String name) throws SQLException
	{
		Operation<java.sql.ResultSet, Timestamp> operation = new Operation<java.sql.ResultSet, Timestamp>()
		{
			public Timestamp execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getTimestamp(name);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getTimestamp(java.lang.String, java.util.Calendar)
	 */
	public Timestamp getTimestamp(final String name, final Calendar calendar) throws SQLException
	{
		Operation<java.sql.ResultSet, Timestamp> operation = new Operation<java.sql.ResultSet, Timestamp>()
		{
			public Timestamp execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getTimestamp(name, calendar);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getType()
	 */
	public int getType() throws SQLException
	{
		Operation<java.sql.ResultSet, Integer> operation = new Operation<java.sql.ResultSet, Integer>()
		{
			public Integer execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getType();
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getUnicodeStream(int)
	 * @deprecated
	 */
	public InputStream getUnicodeStream(final int index) throws SQLException
	{
		Operation<java.sql.ResultSet, InputStream> operation = new Operation<java.sql.ResultSet, InputStream>()
		{
			public InputStream execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getUnicodeStream(index);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getUnicodeStream(java.lang.String)
	 * @deprecated
	 */
	public InputStream getUnicodeStream(final String name) throws SQLException
	{
		Operation<java.sql.ResultSet, InputStream> operation = new Operation<java.sql.ResultSet, InputStream>()
		{
			public InputStream execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getUnicodeStream(name);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getURL(int)
	 */
	public URL getURL(final int index) throws SQLException
	{
		Operation<java.sql.ResultSet, URL> operation = new Operation<java.sql.ResultSet, URL>()
		{
			public URL execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getURL(index);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getURL(java.lang.String)
	 */
	public URL getURL(final String name) throws SQLException
	{
		Operation<java.sql.ResultSet, URL> operation = new Operation<java.sql.ResultSet, URL>()
		{
			public URL execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getURL(name);
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#getWarnings()
	 */
	public SQLWarning getWarnings() throws SQLException
	{
		Operation<java.sql.ResultSet, SQLWarning> operation = new Operation<java.sql.ResultSet, SQLWarning>()
		{
			public SQLWarning execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.getWarnings();
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#insertRow()
	 */
	public void insertRow() throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.insertRow();
				
				return null;
			}
		};
		
		this.executeWriteToDatabase(operation);
	}

	/**
	 * @see java.sql.ResultSet#isAfterLast()
	 */
	public boolean isAfterLast() throws SQLException
	{
		Operation<java.sql.ResultSet, Boolean> operation = new Operation<java.sql.ResultSet, Boolean>()
		{
			public Boolean execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.isAfterLast();
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#isBeforeFirst()
	 */
	public boolean isBeforeFirst() throws SQLException
	{
		Operation<java.sql.ResultSet, Boolean> operation = new Operation<java.sql.ResultSet, Boolean>()
		{
			public Boolean execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.isBeforeFirst();
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#isFirst()
	 */
	public boolean isFirst() throws SQLException
	{
		Operation<java.sql.ResultSet, Boolean> operation = new Operation<java.sql.ResultSet, Boolean>()
		{
			public Boolean execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.isFirst();
			}
		};
		
		return (this.getType() == java.sql.ResultSet.TYPE_SCROLL_SENSITIVE) ? this.executeReadFromDatabase(operation) : this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#isLast()
	 */
	public boolean isLast() throws SQLException
	{
		Operation<java.sql.ResultSet, Boolean> operation = new Operation<java.sql.ResultSet, Boolean>()
		{
			public Boolean execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.isLast();
			}
		};
		
		return (this.getType() == java.sql.ResultSet.TYPE_SCROLL_SENSITIVE) ? this.executeReadFromDatabase(operation) : this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#last()
	 */
	public boolean last() throws SQLException
	{
		Operation<java.sql.ResultSet, Boolean> operation = new Operation<java.sql.ResultSet, Boolean>()
		{
			public Boolean execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.last();
			}
		};
		
		return this.firstValue((this.getType() == java.sql.ResultSet.TYPE_SCROLL_SENSITIVE) ? this.executeWriteToDatabase(operation) : this.executeWriteToDriver(operation));
	}

	/**
	 * @see java.sql.ResultSet#moveToCurrentRow()
	 */
	public void moveToCurrentRow() throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.moveToCurrentRow();
				
				return null;
			}
		};
		
		if (this.getType() == java.sql.ResultSet.TYPE_SCROLL_SENSITIVE)
		{
			this.executeWriteToDatabase(operation);
		}
		else
		{
			this.executeWriteToDriver(operation);
		}
	}

	/**
	 * @see java.sql.ResultSet#moveToInsertRow()
	 */
	public void moveToInsertRow() throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.moveToInsertRow();
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#next()
	 */
	public boolean next() throws SQLException
	{
		Operation<java.sql.ResultSet, Boolean> operation = new Operation<java.sql.ResultSet, Boolean>()
		{
			public Boolean execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.next();
			}
		};
		
		return this.firstValue((this.getType() == java.sql.ResultSet.TYPE_SCROLL_SENSITIVE) ? this.executeWriteToDatabase(operation) : this.executeWriteToDriver(operation));
	}

	/**
	 * @see java.sql.ResultSet#previous()
	 */
	public boolean previous() throws SQLException
	{
		Operation<java.sql.ResultSet, Boolean> operation = new Operation<java.sql.ResultSet, Boolean>()
		{
			public Boolean execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.previous();
			}
		};
		
		return this.firstValue((this.getType() == java.sql.ResultSet.TYPE_SCROLL_SENSITIVE) ? this.executeWriteToDatabase(operation) : this.executeWriteToDriver(operation));
	}

	/**
	 * @see java.sql.ResultSet#refreshRow()
	 */
	public void refreshRow() throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.refreshRow();
				
				return null;
			}
		};
		
		this.executeWriteToDatabase(operation);
	}

	/**
	 * @see java.sql.ResultSet#relative(int)
	 */
	public boolean relative(final int index) throws SQLException
	{
		Operation<java.sql.ResultSet, Boolean> operation = new Operation<java.sql.ResultSet, Boolean>()
		{
			public Boolean execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.relative(index);
			}
		};
		
		return this.firstValue(this.executeWriteToDriver(operation));
	}

	/**
	 * @see java.sql.ResultSet#rowDeleted()
	 */
	public boolean rowDeleted() throws SQLException
	{
		Operation<java.sql.ResultSet, Boolean> operation = new Operation<java.sql.ResultSet, Boolean>()
		{
			public Boolean execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.rowDeleted();
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#rowInserted()
	 */
	public boolean rowInserted() throws SQLException
	{
		Operation<java.sql.ResultSet, Boolean> operation = new Operation<java.sql.ResultSet, Boolean>()
		{
			public Boolean execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.rowInserted();
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#rowUpdated()
	 */
	public boolean rowUpdated() throws SQLException
	{
		Operation<java.sql.ResultSet, Boolean> operation = new Operation<java.sql.ResultSet, Boolean>()
		{
			public Boolean execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.rowUpdated();
			}
		};
		
		return this.executeReadFromDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#setFetchDirection(int)
	 */
	public void setFetchDirection(final int direction) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.setFetchDirection(direction);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#setFetchSize(int)
	 */
	public void setFetchSize(final int size) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.setFetchSize(size);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateArray(int, java.sql.Array)
	 */
	public void updateArray(final int index, final Array value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateArray(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateArray(java.lang.String, java.sql.Array)
	 */
	public void updateArray(final String name, final Array value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateArray(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateAsciiStream(int, java.io.InputStream, int)
	 */
	public void updateAsciiStream(final int index, InputStream inputStream, final int length) throws SQLException
	{
		final FileSupport fileSupport = this.getFileSupport();
		final File file = fileSupport.createFile(inputStream);
		
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateAsciiStream(index, fileSupport.getInputStream(file), length);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateAsciiStream(java.lang.String, java.io.InputStream, int)
	 */
	public void updateAsciiStream(final String name, InputStream inputStream, final int length) throws SQLException
	{
		final FileSupport fileSupport = this.getFileSupport();
		final File file = fileSupport.createFile(inputStream);
		
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateAsciiStream(name, fileSupport.getInputStream(file), length);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateBigDecimal(int, java.math.BigDecimal)
	 */
	public void updateBigDecimal(final int index, final BigDecimal value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateBigDecimal(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateBigDecimal(java.lang.String, java.math.BigDecimal)
	 */
	public void updateBigDecimal(final String name, final BigDecimal value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateBigDecimal(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateBinaryStream(int, java.io.InputStream, int)
	 */
	public void updateBinaryStream(final int index, InputStream inputStream, final int length) throws SQLException
	{
		final FileSupport fileSupport = this.getFileSupport();
		final File file = fileSupport.createFile(inputStream);
		
		try
		{
			Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
			{
				public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
				{
					resultSet.updateBinaryStream(index, fileSupport.getInputStream(file), length);
					
					return null;
				}
			};
			
			this.executeWriteToDriver(operation);
		}
		finally
		{
			file.delete();
		}
	}

	/**
	 * @see java.sql.ResultSet#updateBinaryStream(java.lang.String, java.io.InputStream, int)
	 */
	public void updateBinaryStream(final String name, InputStream inputStream, final int length) throws SQLException
	{
		final FileSupport fileSupport = this.getFileSupport();
		final File file = fileSupport.createFile(inputStream);
		
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateBinaryStream(name, fileSupport.getInputStream(file), length);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateBlob(int, java.sql.Blob)
	 */
	public void updateBlob(final int index, final java.sql.Blob value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateBlob(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateBlob(java.lang.String, java.sql.Blob)
	 */
	public void updateBlob(final String name, final java.sql.Blob value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateBlob(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateBoolean(int, boolean)
	 */
	public void updateBoolean(final int index, final boolean value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateBoolean(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateBoolean(java.lang.String, boolean)
	 */
	public void updateBoolean(final String name, final boolean value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateBoolean(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateByte(int, byte)
	 */
	public void updateByte(final int index, final byte value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateByte(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateByte(java.lang.String, byte)
	 */
	public void updateByte(final String name, final byte value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateByte(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateBytes(int, byte[])
	 */
	public void updateBytes(final int index, final byte[] value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateBytes(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateBytes(java.lang.String, byte[])
	 */
	public void updateBytes(final String name, final byte[] value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateBytes(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateCharacterStream(int, java.io.Reader, int)
	 */
	public void updateCharacterStream(final int index, Reader reader, final int length) throws SQLException
	{
		final FileSupport fileSupport = this.getFileSupport();
		final File file = fileSupport.createFile(reader);
		
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateCharacterStream(index, fileSupport.getReader(file), length);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateCharacterStream(java.lang.String, java.io.Reader, int)
	 */
	public void updateCharacterStream(final String name, Reader reader, final int length) throws SQLException
	{
		final FileSupport fileSupport = this.getFileSupport();
		final File file = fileSupport.createFile(reader);
		
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateCharacterStream(name, fileSupport.getReader(file), length);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateClob(int, java.sql.Clob)
	 */
	public void updateClob(final int index, final java.sql.Clob value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateClob(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateClob(java.lang.String, java.sql.Clob)
	 */
	public void updateClob(final String name, final java.sql.Clob value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateClob(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateDate(int, java.sql.Date)
	 */
	public void updateDate(final int index, final Date value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateDate(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateDate(java.lang.String, java.sql.Date)
	 */
	public void updateDate(final String name, final Date value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateDate(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateDouble(int, double)
	 */
	public void updateDouble(final int index, final double value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateDouble(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateDouble(java.lang.String, double)
	 */
	public void updateDouble(final String name, final double value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateDouble(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateFloat(int, float)
	 */
	public void updateFloat(final int index, final float value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateFloat(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateFloat(java.lang.String, float)
	 */
	public void updateFloat(final String name, final float value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateFloat(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateInt(int, int)
	 */
	public void updateInt(final int index, final int value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateInt(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateInt(java.lang.String, int)
	 */
	public void updateInt(final String name, final int value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateInt(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateLong(int, long)
	 */
	public void updateLong(final int index, final long value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateLong(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateLong(java.lang.String, long)
	 */
	public void updateLong(final String name, final long value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateLong(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateNull(int)
	 */
	public void updateNull(final int index) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateNull(index);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateNull(java.lang.String)
	 */
	public void updateNull(final String name) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateNull(name);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateObject(int, java.lang.Object)
	 */
	public void updateObject(final int index, final Object value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateObject(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateObject(int, java.lang.Object, int)
	 */
	public void updateObject(final int index, final Object value, final int sqlType) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateObject(index, value, sqlType);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateObject(java.lang.String, java.lang.Object)
	 */
	public void updateObject(final String name, final Object value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateObject(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateObject(java.lang.String, java.lang.Object, int)
	 */
	public void updateObject(final String name, final Object value, final int sqlType) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateObject(name, value, sqlType);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateRef(int, java.sql.Ref)
	 */
	public void updateRef(final int index, final Ref value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateRef(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateRef(java.lang.String, java.sql.Ref)
	 */
	public void updateRef(final String name, final Ref value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateRef(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateRow()
	 */
	public void updateRow() throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateRow();
				
				return null;
			}
		};
		
		this.executeWriteToDatabase(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateShort(int, short)
	 */
	public void updateShort(final int index, final short value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateShort(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateShort(java.lang.String, short)
	 */
	public void updateShort(final String name, final short value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateShort(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateString(int, java.lang.String)
	 */
	public void updateString(final int index, final String value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateString(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateString(java.lang.String, java.lang.String)
	 */
	public void updateString(final String name, final String value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateString(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateTime(int, java.sql.Time)
	 */
	public void updateTime(final int index, final Time value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateTime(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateTime(java.lang.String, java.sql.Time)
	 */
	public void updateTime(final String name, final Time value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateTime(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateTimestamp(int, java.sql.Timestamp)
	 */
	public void updateTimestamp(final int index, final Timestamp value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateTimestamp(index, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#updateTimestamp(java.lang.String, java.sql.Timestamp)
	 */
	public void updateTimestamp(final String name, final Timestamp value) throws SQLException
	{
		Operation<java.sql.ResultSet, Void> operation = new Operation<java.sql.ResultSet, Void>()
		{
			public Void execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				resultSet.updateTimestamp(name, value);
				
				return null;
			}
		};
		
		this.executeWriteToDriver(operation);
	}

	/**
	 * @see java.sql.ResultSet#wasNull()
	 */
	public boolean wasNull() throws SQLException
	{
		Operation<java.sql.ResultSet, Boolean> operation = new Operation<java.sql.ResultSet, Boolean>()
		{
			public Boolean execute(Database database, java.sql.ResultSet resultSet) throws SQLException
			{
				return resultSet.wasNull();
			}
		};
		
		return this.executeReadFromDriver(operation);
	}
	
	protected FileSupport getFileSupport() throws SQLException
	{
		Connection connection = (Connection) this.getStatement().getConnection();
		
		return connection.getFileSupport();
	}
}
