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
package net.sf.hajdbc.sql;

import java.io.ByteArrayInputStream;
import java.io.CharArrayReader;
import java.io.File;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.util.reflect.ProxyFactory;

import org.easymock.EasyMock;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test for {@link CallableStatement}.
 * 
 * @author  Paul Ferraro
 * @since   1.1
 */
@SuppressWarnings("unchecked")
public class TestCallableStatement extends TestPreparedStatement implements java.sql.CallableStatement
{
	@Override
	protected Class<? extends java.sql.CallableStatement> getStatementClass()
	{
		return java.sql.CallableStatement.class;
	}
	
	protected Class<? extends java.sql.CallableStatement> getCallableStatementClass()
	{
		return java.sql.CallableStatement.class;
	}
	
	private CallableStatement getStatement()
	{
		return CallableStatement.class.cast(this.statement);
	}
	
	private CallableStatement getStatement1()
	{
		return CallableStatement.class.cast(this.statement1);
	}
	
	private CallableStatement getStatement2()
	{
		return CallableStatement.class.cast(this.statement2);
	}
	
	@Override
	protected AbstractStatementInvocationHandler getInvocationHandler(Map map) throws Exception
	{
		return new CallableStatementInvocationHandler(this.connection, this.parent, EasyMock.createMock(Invoker.class), map, this.fileSupport, this.sql);
	}
	
	/**
	 * @see net.sf.hajdbc.sql.TestStatement#recordConstructor()
	 */
	@Override
	protected void recordConstructor() throws SQLException
	{
		this.parent.addChild(EasyMock.isA(CallableStatementInvocationHandler.class));
		
		this.expectLocks(this.sql, null, null);
		this.expectSelectForUpdateCheck(this.sql, false);
	}
	
	/**
	 * @see java.sql.CallableStatement#getArray(int)
	 */
	@Test(dataProvider = "int")
	public Array getArray(int index) throws SQLException
	{
		Array array = EasyMock.createMock(Array.class);
		
		EasyMock.expect(this.getStatement1().getArray(index)).andReturn(array);
		
		this.replay();
		
		Array value = this.getStatement().getArray(index);
		
		this.verify();
		
		assert value == array;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getArray(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public Array getArray(String name) throws SQLException
	{
		Array array = EasyMock.createMock(Array.class);
		
		EasyMock.expect(this.getStatement1().getArray(name)).andReturn(array);
		
		this.replay();
		
		Array value = this.getStatement().getArray(name);
		
		this.verify();
		
		assert value == array;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getBigDecimal(int)
	 */
	@Test(dataProvider = "int")
	public BigDecimal getBigDecimal(int index) throws SQLException
	{
		BigDecimal decimal = new BigDecimal(10);
		
		EasyMock.expect(this.getStatement1().getBigDecimal(index)).andReturn(decimal);
		
		this.replay();
		
		BigDecimal value = this.getStatement().getBigDecimal(index);
		
		this.verify();
		
		assert value == decimal;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getBigDecimal(int, int)
	 */
	@SuppressWarnings("deprecation")
	@Test(dataProvider = "int-int")
	@Deprecated
	public BigDecimal getBigDecimal(int index, int scale) throws SQLException
	{
		BigDecimal decimal = new BigDecimal(10);
		
		EasyMock.expect(this.getStatement1().getBigDecimal(index, scale)).andReturn(decimal);
		
		this.replay();
		
		BigDecimal value = this.getStatement().getBigDecimal(index, scale);
		
		this.verify();
		
		assert value == decimal;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getBigDecimal(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public BigDecimal getBigDecimal(String name) throws SQLException
	{
		BigDecimal decimal = new BigDecimal(10);
		
		EasyMock.expect(this.getStatement1().getBigDecimal(name)).andReturn(decimal);
		
		this.replay();
		
		BigDecimal value = this.getStatement().getBigDecimal(name);
		
		this.verify();
		
		assert value == decimal;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getBlob(int)
	 */
	@Test(dataProvider = "int")
	public Blob getBlob(int index) throws SQLException
	{
		Blob blob = EasyMock.createMock(Blob.class);
		
		EasyMock.expect(this.getStatement1().getBlob(index)).andReturn(blob);
		
		this.replay();
		
		Blob value = this.getStatement().getBlob(index);
		
		this.verify();
		
		assert value == blob;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getBlob(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public Blob getBlob(String name) throws SQLException
	{
		Blob blob = EasyMock.createMock(Blob.class);
		
		EasyMock.expect(this.getStatement1().getBlob(name)).andReturn(blob);
		
		this.replay();
		
		Blob value = this.getStatement().getBlob(name);
		
		this.verify();
		
		assert value == blob;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getBoolean(int)
	 */
	@Test(dataProvider = "int")
	public boolean getBoolean(int index) throws SQLException
	{
		EasyMock.expect(this.getStatement1().getBoolean(index)).andReturn(true);
		
		this.replay();
		
		boolean value = this.getStatement().getBoolean(index);
		
		this.verify();
		
		assert value;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getBoolean(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public boolean getBoolean(String name) throws SQLException
	{
		EasyMock.expect(this.getStatement1().getBoolean(name)).andReturn(true);
		
		this.replay();
		
		boolean value = this.getStatement().getBoolean(name);
		
		this.verify();
		
		assert value;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getByte(int)
	 */
	@Test(dataProvider = "int")
	public byte getByte(int index) throws SQLException
	{
		byte b = Integer.valueOf(1).byteValue();
		
		EasyMock.expect(this.getStatement1().getByte(index)).andReturn(b);
		
		this.replay();
		
		byte value = this.getStatement().getByte(index);
		
		this.verify();
		
		assert value == b;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getByte(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public byte getByte(String name) throws SQLException
	{
		byte b = Integer.valueOf(1).byteValue();
		
		EasyMock.expect(this.getStatement1().getByte(name)).andReturn(b);
		
		this.replay();
		
		byte value = this.getStatement().getByte(name);
		
		this.verify();
		
		assert value == b;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getBytes(int)
	 */
	@Test(dataProvider = "int")
	public byte[] getBytes(int index) throws SQLException
	{
		byte[] bytes = new byte[0];
		
		EasyMock.expect(this.getStatement1().getBytes(index)).andReturn(bytes);
		
		this.replay();
		
		byte[] value = this.getStatement().getBytes(index);
		
		this.verify();
		
		assert value == bytes;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getBytes(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public byte[] getBytes(String name) throws SQLException
	{
		byte[] bytes = new byte[0];
		
		EasyMock.expect(this.getStatement1().getBytes(name)).andReturn(bytes);
		
		this.replay();
		
		byte[] value = this.getStatement().getBytes(name);
		
		this.verify();
		
		assert value == bytes;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getClob(int)
	 */
	@Test(dataProvider = "int")
	public Clob getClob(int index) throws SQLException
	{
		Clob clob = EasyMock.createMock(Clob.class);
		
		EasyMock.expect(this.getStatement1().getClob(index)).andReturn(clob);
		
		this.replay();
		
		Clob value = this.getStatement().getClob(index);
		
		this.verify();
		
		assert value == clob;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getClob(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public Clob getClob(String name) throws SQLException
	{
		Clob clob = EasyMock.createMock(Clob.class);
		
		EasyMock.expect(this.getStatement1().getClob(name)).andReturn(clob);
		
		this.replay();
		
		Clob value = this.getStatement().getClob(name);
		
		this.verify();
		
		assert value == clob;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getDate(int)
	 */
	@Test(dataProvider = "int")
	public Date getDate(int index) throws SQLException
	{
		Date date = new Date(System.currentTimeMillis());
		
		EasyMock.expect(this.getStatement1().getDate(index)).andReturn(date);
		
		this.replay();
		
		Date value = this.getStatement().getDate(index);
		
		this.verify();
		
		assert value == date;
		
		return value;
	}

	@DataProvider(name = "int-calendar")
	Object[][] intCalendarProvider()
	{
		return new Object[][] { new Object[] { 1, Calendar.getInstance() } };
	}
	
	/**
	 * @see java.sql.CallableStatement#getDate(int, java.util.Calendar)
	 */
	@Test(dataProvider = "int-calendar")
	public Date getDate(int index, Calendar calendar) throws SQLException
	{
		Date date = new Date(System.currentTimeMillis());
		
		EasyMock.expect(this.getStatement1().getDate(index, calendar)).andReturn(date);
		
		this.replay();
		
		Date value = this.getStatement().getDate(index, calendar);
		
		this.verify();
		
		assert value == date;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getDate(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public Date getDate(String name) throws SQLException
	{
		Date date = new Date(System.currentTimeMillis());
		
		EasyMock.expect(this.getStatement1().getDate(name)).andReturn(date);
		
		this.replay();
		
		Date value = this.getStatement().getDate(name);
		
		this.verify();
		
		assert value == date;
		
		return value;
	}

	@DataProvider(name = "string-calendar")
	Object[][] stringCalendarProvider()
	{
		return new Object[][] { new Object[] { "column", Calendar.getInstance() } };
	}

	/**
	 * @see java.sql.CallableStatement#getDate(java.lang.String, java.util.Calendar)
	 */
	@Test(dataProvider = "string-calendar")
	public Date getDate(String name, Calendar calendar) throws SQLException
	{
		Date date = new Date(System.currentTimeMillis());
		
		EasyMock.expect(this.getStatement1().getDate(name, calendar)).andReturn(date);
		
		this.replay();
		
		Date value = this.getStatement().getDate(name, calendar);
		
		this.verify();
		
		assert value == date;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getDouble(int)
	 */
	@Test(dataProvider = "int")
	public double getDouble(int index) throws SQLException
	{
		double d = 1.0;
		
		EasyMock.expect(this.getStatement1().getDouble(index)).andReturn(d);
		
		this.replay();
		
		double value = this.getStatement().getDouble(index);
		
		this.verify();
		
		assert value == d;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getDouble(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public double getDouble(String name) throws SQLException
	{
		double d = 1.0;
		
		EasyMock.expect(this.getStatement1().getDouble(name)).andReturn(d);
		
		this.replay();
		
		double value = this.getStatement().getDouble(name);
		
		this.verify();
		
		assert value == d;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getFloat(int)
	 */
	@Test(dataProvider = "int")
	public float getFloat(int index) throws SQLException
	{
		float f = 1.0f;
		
		EasyMock.expect(this.getStatement1().getFloat(index)).andReturn(f);
		
		this.replay();
		
		float value = this.getStatement().getFloat(index);
		
		this.verify();
		
		assert value == f;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getFloat(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public float getFloat(String name) throws SQLException
	{
		float f = 1.0f;
		
		EasyMock.expect(this.getStatement1().getFloat(name)).andReturn(f);
		
		this.replay();
		
		float value = this.getStatement().getFloat(name);
		
		this.verify();
		
		assert value == f;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getInt(int)
	 */
	@Test(dataProvider = "int")
	public int getInt(int index) throws SQLException
	{
		int i = 1;
		
		EasyMock.expect(this.getStatement1().getInt(index)).andReturn(i);
		
		this.replay();
		
		int value = this.getStatement().getInt(index);
		
		this.verify();
		
		assert value == i;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getInt(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public int getInt(String name) throws SQLException
	{
		int i = 1;
		
		EasyMock.expect(this.getStatement1().getInt(name)).andReturn(i);
		
		this.replay();
		
		int value = this.getStatement().getInt(name);
		
		this.verify();
		
		assert value == i;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getLong(int)
	 */
	@Test(dataProvider = "int")
	public long getLong(int index) throws SQLException
	{
		long i = 1;
		
		EasyMock.expect(this.getStatement1().getLong(index)).andReturn(i);
		
		this.replay();
		
		long value = this.getStatement().getLong(index);
		
		this.verify();
		
		assert value == i;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getLong(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public long getLong(String name) throws SQLException
	{
		long i = 1;
		
		EasyMock.expect(this.getStatement1().getLong(name)).andReturn(i);
		
		this.replay();
		
		long value = this.getStatement().getLong(name);
		
		this.verify();
		
		assert value == i;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getObject(int)
	 */
	@Test(dataProvider = "int")
	public Object getObject(int index) throws SQLException
	{
		Object object = new Object();
		
		EasyMock.expect(this.getStatement1().getObject(index)).andReturn(object);
		
		this.replay();
		
		Object value = this.getStatement().getObject(index);
		
		this.verify();
		
		assert value == object;
		
		return value;
	}

	@DataProvider(name = "int-map")
	Object[][] intMapProvider()
	{
		return new Object[][] { new Object[] { 1, Collections.EMPTY_MAP } };
	}
	
	/**
	 * @see java.sql.CallableStatement#getObject(int, java.util.Map)
	 */
	public Object getObject(int index, Map<String, Class<?>> map) throws SQLException
	{
		Object object = new Object();
		
		EasyMock.expect(this.getStatement1().getObject(index, map)).andReturn(object);
		
		this.replay();
		
		Object value = this.getStatement().getObject(index, map);
		
		this.verify();
		
		assert value == object;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getObject(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public Object getObject(String name) throws SQLException
	{
		Object object = new Object();
		
		EasyMock.expect(this.getStatement1().getObject(name)).andReturn(object);
		
		this.replay();
		
		Object value = this.getStatement().getObject(name);
		
		this.verify();
		
		assert value == object;
		
		return value;
	}

	@DataProvider(name = "string-map")
	Object[][] stringMapProvider()
	{
		return new Object[][] { new Object[] { "column", Collections.EMPTY_MAP } };
	}

	/**
	 * @see java.sql.CallableStatement#getObject(java.lang.String, java.util.Map)
	 */
	@Test(dataProvider = "string-map")
	public Object getObject(String name, Map<String, Class<?>> map) throws SQLException
	{
		Object object = new Object();
		
		EasyMock.expect(this.getStatement1().getObject(name, map)).andReturn(object);
		
		this.replay();
		
		Object value = this.getStatement().getObject(name, map);
		
		this.verify();
		
		assert value == object;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getRef(int)
	 */
	@Test(dataProvider = "int")
	public Ref getRef(int index) throws SQLException
	{
		Ref ref = EasyMock.createMock(Ref.class);
		
		EasyMock.expect(this.getStatement1().getRef(index)).andReturn(ref);
		
		this.replay();
		
		Ref value = this.getStatement().getRef(index);
		
		this.verify();
		
		assert value == ref;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getRef(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public Ref getRef(String name) throws SQLException
	{
		Ref ref = EasyMock.createMock(Ref.class);
		
		EasyMock.expect(this.getStatement1().getRef(name)).andReturn(ref);
		
		this.replay();
		
		Ref value = this.getStatement().getRef(name);
		
		this.verify();
		
		assert value == ref;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getShort(int)
	 */
	@Test(dataProvider = "int")
	public short getShort(int index) throws SQLException
	{
		short s = Integer.valueOf(1).shortValue();
		
		EasyMock.expect(this.getStatement1().getShort(index)).andReturn(s);
		
		this.replay();
		
		short value = this.getStatement().getShort(index);
		
		this.verify();
		
		assert value == s;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getShort(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public short getShort(String name) throws SQLException
	{
		short s = Integer.valueOf(1).shortValue();
		
		EasyMock.expect(this.getStatement1().getShort(name)).andReturn(s);
		
		this.replay();
		
		short value = this.getStatement().getShort(name);
		
		this.verify();
		
		assert value == s;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getString(int)
	 */
	@Test(dataProvider = "int")
	public String getString(int index) throws SQLException
	{
		String s = "";
		
		EasyMock.expect(this.getStatement1().getString(index)).andReturn(s);
		
		this.replay();
		
		String value = this.getStatement().getString(index);
		
		this.verify();
		
		assert value == s;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getString(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public String getString(String name) throws SQLException
	{
		String s = "";
		
		EasyMock.expect(this.getStatement1().getString(name)).andReturn(s);
		
		this.replay();
		
		String value = this.getStatement().getString(name);
		
		this.verify();
		
		assert value == s;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getTime(int)
	 */
	@Test(dataProvider = "int")
	public Time getTime(int index) throws SQLException
	{
		Time time = new Time(System.currentTimeMillis());
		
		EasyMock.expect(this.getStatement1().getTime(index)).andReturn(time);
		
		this.replay();
		
		Time value = this.getStatement().getTime(index);
		
		this.verify();
		
		assert value == time;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getTime(int, java.util.Calendar)
	 */
	@Test(dataProvider = "int-calendar")
	public Time getTime(int index, Calendar calendar) throws SQLException
	{
		Time time = new Time(System.currentTimeMillis());
		
		EasyMock.expect(this.getStatement1().getTime(index, calendar)).andReturn(time);
		
		this.replay();
		
		Time value = this.getStatement().getTime(index, calendar);
		
		this.verify();
		
		assert value == time;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getTime(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public Time getTime(String name) throws SQLException
	{
		Time time = new Time(System.currentTimeMillis());
		
		EasyMock.expect(this.getStatement1().getTime(name)).andReturn(time);
		
		this.replay();
		
		Time value = this.getStatement().getTime(name);
		
		this.verify();
		
		assert value == time;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getTime(java.lang.String, java.util.Calendar)
	 */
	@Test(dataProvider = "string-calendar")
	public Time getTime(String name, Calendar calendar) throws SQLException
	{
		Time time = new Time(System.currentTimeMillis());
		
		EasyMock.expect(this.getStatement1().getTime(name, calendar)).andReturn(time);
		
		this.replay();
		
		Time value = this.getStatement().getTime(name, calendar);
		
		this.verify();
		
		assert value == time;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getTimestamp(int)
	 */
	@Test(dataProvider = "int")
	public Timestamp getTimestamp(int index) throws SQLException
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		
		EasyMock.expect(this.getStatement1().getTimestamp(index)).andReturn(timestamp);
		
		this.replay();
		
		Timestamp value = this.getStatement().getTimestamp(index);
		
		this.verify();
		
		assert value == timestamp;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getTimestamp(int, java.util.Calendar)
	 */
	@Test(dataProvider = "int-calendar")
	public Timestamp getTimestamp(int index, Calendar cal) throws SQLException
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		
		EasyMock.expect(this.getStatement1().getTimestamp(index)).andReturn(timestamp);
		
		this.replay();
		
		Timestamp value = this.getStatement().getTimestamp(index);
		
		this.verify();
		
		assert value == timestamp;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getTimestamp(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public Timestamp getTimestamp(String name) throws SQLException
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		
		EasyMock.expect(this.getStatement1().getTimestamp(name)).andReturn(timestamp);
		
		this.replay();
		
		Timestamp value = this.getStatement().getTimestamp(name);
		
		this.verify();
		
		assert value == timestamp;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getTimestamp(java.lang.String, java.util.Calendar)
	 */
	@Test(dataProvider = "string-calendar")
	public Timestamp getTimestamp(String name, Calendar calendar) throws SQLException
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		
		EasyMock.expect(this.getStatement1().getTimestamp(name, calendar)).andReturn(timestamp);
		
		this.replay();
		
		Timestamp value = this.getStatement().getTimestamp(name, calendar);
		
		this.verify();
		
		assert value == timestamp;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getURL(int)
	 */
	@Test(dataProvider = "int")
	public URL getURL(int index) throws SQLException
	{
		try
		{
			URL url = new URL("http://ha-jdbc.sf.net");
			
			EasyMock.expect(this.getStatement1().getURL(index)).andReturn(url);
			
			this.replay();
			
			URL value = this.getStatement().getURL(index);
			
			this.verify();
			
			assert value == url;
			
			return value;
		}
		catch (MalformedURLException e)
		{
			assert false : e;
			return null;
		}
	}

	/**
	 * @see java.sql.CallableStatement#getURL(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public URL getURL(String name) throws SQLException
	{
		try
		{
			URL url = new URL("http://ha-jdbc.sf.net");
			
			EasyMock.expect(this.getStatement1().getURL(name)).andReturn(url);
			
			this.replay();
			
			URL value = this.getStatement().getURL(name);
			
			this.verify();
			
			assert value == url;
			
			return value;
		}
		catch (MalformedURLException e)
		{
			assert false : e;
			return null;
		}
	}

	/**
	 * @see java.sql.CallableStatement#registerOutParameter(int, int)
	 */
	@Test(dataProvider = "int-int")
	public void registerOutParameter(int index, int sqlType) throws SQLException
	{
		this.getStatement1().registerOutParameter(index, sqlType);
		this.getStatement2().registerOutParameter(index, sqlType);
		
		this.replay();
		
		this.getStatement().registerOutParameter(index, sqlType);
		
		this.verify();
	}
	
	@DataProvider(name = "int-int-int")
	Object[][] intIntIntProvider()
	{
		return new Object[][] { new Object[] { 1, Types.INTEGER, 1 } };
	}

	/**
	 * @see java.sql.CallableStatement#registerOutParameter(int, int, int)
	 */
	@Test(dataProvider = "int-int-int")
	public void registerOutParameter(int index, int sqlType, int scale) throws SQLException
	{
		this.getStatement1().registerOutParameter(index, sqlType, scale);
		this.getStatement2().registerOutParameter(index, sqlType, scale);
		
		this.replay();
		
		this.getStatement().registerOutParameter(index, sqlType, scale);
		
		this.verify();
	}
	
	/**
	 * @see java.sql.CallableStatement#registerOutParameter(int, int, java.lang.String)
	 */
	@Test(dataProvider = "int-int-string")
	public void registerOutParameter(int index, int sqlType, String typeName) throws SQLException
	{
		this.getStatement1().registerOutParameter(index, sqlType, typeName);
		this.getStatement2().registerOutParameter(index, sqlType, typeName);
		
		this.replay();
		
		this.getStatement().registerOutParameter(index, sqlType, typeName);
		
		this.verify();
	}
	
	/**
	 * @see java.sql.CallableStatement#registerOutParameter(java.lang.String, int)
	 */
	@Test(dataProvider = "string-int")
	public void registerOutParameter(String name, int sqlType) throws SQLException
	{
		this.getStatement1().registerOutParameter(name, sqlType);
		this.getStatement2().registerOutParameter(name, sqlType);
		
		this.replay();
		
		this.getStatement().registerOutParameter(name, sqlType);
		
		this.verify();
	}

	@DataProvider(name = "string-int-int")
	Object[][] stringIntIntProvider()
	{
		return new Object[][] { new Object[] { "column", Types.INTEGER, 1 } };
	}

	/**
	 * @see java.sql.CallableStatement#registerOutParameter(java.lang.String, int, int)
	 */
	@Test(dataProvider = "string-int-int")
	public void registerOutParameter(String name, int sqlType, int scale) throws SQLException
	{
		this.getStatement1().registerOutParameter(name, sqlType, scale);
		this.getStatement2().registerOutParameter(name, sqlType, scale);
		
		this.replay();
		
		this.getStatement().registerOutParameter(name, sqlType, scale);
		
		this.verify();
	}

	@DataProvider(name = "string-int-string")
	Object[][] stringIntStringProvider()
	{
		return new Object[][] { new Object[] { "column", Types.INTEGER, "int" } };
	}
	
	/**
	 * @see java.sql.CallableStatement#registerOutParameter(java.lang.String, int, java.lang.String)
	 */
	@Test(dataProvider = "string-int-string")
	public void registerOutParameter(String name, int sqlType, String typeName) throws SQLException
	{
		this.getStatement1().registerOutParameter(name, sqlType, typeName);
		this.getStatement2().registerOutParameter(name, sqlType, typeName);
		
		this.replay();
		
		this.getStatement().registerOutParameter(name, sqlType, typeName);
		
		this.verify();
	}

	@DataProvider(name = "string-inputStream-int")
	Object[][] stringInputStreamIntProvider()
	{
		return new Object[][] { new Object[] { "column", new ByteArrayInputStream(new byte[0]), 0 } };
	}
	
	/**
	 * @see java.sql.CallableStatement#setAsciiStream(java.lang.String, java.io.InputStream, int)
	 */
	@Test(dataProvider = "string-inputStream-int")
	public void setAsciiStream(String name, InputStream value, int length) throws SQLException
	{
		InputStream inputStream1 = new ByteArrayInputStream(new byte[0]);
		InputStream inputStream2 = new ByteArrayInputStream(new byte[0]);
		File file = new File("");
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(inputStream1);
		
		this.getStatement1().setAsciiStream(name, inputStream1, length);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(inputStream2);
		
		this.getStatement2().setAsciiStream(name, inputStream2, length);
		
		this.replay();
		
		this.getStatement().setAsciiStream(name, value, length);
		
		this.verify();
	}

	@DataProvider(name = "string-bigDecimal")
	Object[][] stringBigDecimalProvider()
	{
		return new Object[][] { new Object[] { "column", new BigDecimal(1.0) } };
	}

	/**
	 * @see java.sql.CallableStatement#setBigDecimal(java.lang.String, java.math.BigDecimal)
	 */
	@Test(dataProvider = "string-bigDecimal")
	public void setBigDecimal(String name, BigDecimal value) throws SQLException
	{
		this.getStatement1().setBigDecimal(name, value);
		this.getStatement2().setBigDecimal(name, value);
		
		this.replay();
		
		this.getStatement().setBigDecimal(name, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.CallableStatement#setBinaryStream(java.lang.String, java.io.InputStream, int)
	 */
	@Test(dataProvider = "string-inputStream-int")
	public void setBinaryStream(String name, InputStream value, int length) throws SQLException
	{
		InputStream inputStream1 = new ByteArrayInputStream(new byte[0]);
		InputStream inputStream2 = new ByteArrayInputStream(new byte[0]);
		File file = new File("");
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(inputStream1);
		
		this.getStatement1().setBinaryStream(name, inputStream1, length);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(inputStream2);
		
		this.getStatement2().setBinaryStream(name, inputStream2, length);
		
		this.replay();
		
		this.getStatement().setBinaryStream(name, value, length);
		
		this.verify();
	}

	@DataProvider(name = "string-boolean")
	Object[][] stringBooleanProvider()
	{
		return new Object[][] { new Object[] { "column", true } };
	}
	
	/**
	 * @see java.sql.CallableStatement#setBoolean(java.lang.String, boolean)
	 */
	@Test(dataProvider = "string-boolean")
	public void setBoolean(String name, boolean value) throws SQLException
	{
		this.getStatement1().setBoolean(name, value);
		this.getStatement2().setBoolean(name, value);
		
		this.replay();
		
		this.getStatement().setBoolean(name, value);
		
		this.verify();
	}

	@DataProvider(name = "string-byte")
	Object[][] stringByteProvider()
	{
		return new Object[][] { new Object[] { "column", Integer.valueOf(1).byteValue() } };
	}

	/**
	 * @see java.sql.CallableStatement#setByte(java.lang.String, byte)
	 */
	@Test(dataProvider = "string-byte")
	public void setByte(String name, byte value) throws SQLException
	{
		this.getStatement1().setByte(name, value);
		this.getStatement2().setByte(name, value);
		
		this.replay();
		
		this.getStatement().setByte(name, value);
		
		this.verify();
	}

	@DataProvider(name = "string-bytes")
	Object[][] stringBytesProvider()
	{
		return new Object[][] { new Object[] { "column", new byte[0] } };
	}

	/**
	 * @see java.sql.CallableStatement#setBytes(java.lang.String, byte[])
	 */
	@Test(dataProvider = "string-bytes")
	public void setBytes(String name, byte[] value) throws SQLException
	{
		this.getStatement1().setBytes(name, value);
		this.getStatement2().setBytes(name, value);
		
		this.replay();
		
		this.getStatement().setBytes(name, value);
		
		this.verify();
	}

	@DataProvider(name = "string-reader-int")
	Object[][] stringReaderIntProvider()
	{
		return new Object[][] { new Object[] { "column", new CharArrayReader(new char[0]), 0 } };
	}

	/**
	 * @see java.sql.CallableStatement#setCharacterStream(java.lang.String, java.io.Reader, int)
	 */
	@Test(dataProvider = "string-reader-int")
	public void setCharacterStream(String name, Reader value, int length) throws SQLException
	{
		Reader reader1 = new CharArrayReader(new char[0]);
		Reader reader2 = new CharArrayReader(new char[0]);
		File file = new File("");
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.getStatement1().setCharacterStream(name, reader1, length);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.getStatement2().setCharacterStream(name, reader2, length);
		
		this.replay();
		
		this.getStatement().setCharacterStream(name, value, length);
		
		this.verify();
	}

	@DataProvider(name = "string-date")
	Object[][] stringDateProvider()
	{
		return new Object[][] { new Object[] { "column", new Date(System.currentTimeMillis()) } };
	}

	/**
	 * @see java.sql.CallableStatement#setDate(java.lang.String, java.sql.Date)
	 */
	@Test(dataProvider = "string-date")
	public void setDate(String name, Date value) throws SQLException
	{
		this.getStatement1().setDate(name, value);
		this.getStatement2().setDate(name, value);
		
		this.replay();
		
		this.getStatement().setDate(name, value);
		
		this.verify();
	}

	@DataProvider(name = "string-date-calendar")
	Object[][] stringDateCalendarProvider()
	{
		return new Object[][] { new Object[] { "column", new Date(System.currentTimeMillis()), Calendar.getInstance() } };
	}

	/**
	 * @see java.sql.CallableStatement#setDate(java.lang.String, java.sql.Date, java.util.Calendar)
	 */
	@Test(dataProvider = "string-date-calendar")
	public void setDate(String name, Date value, Calendar calendar) throws SQLException
	{
		this.getStatement1().setDate(name, value, calendar);
		this.getStatement2().setDate(name, value, calendar);
		
		this.replay();
		
		this.getStatement().setDate(name, value, calendar);
		
		this.verify();
	}

	@DataProvider(name = "string-double")
	Object[][] stringDoubleProvider()
	{
		return new Object[][] { new Object[] { "column", 1.0 } };
	}

	/**
	 * @see java.sql.CallableStatement#setDouble(java.lang.String, double)
	 */
	@Test(dataProvider = "string-double")
	public void setDouble(String name, double value) throws SQLException
	{
		this.getStatement1().setDouble(name, value);
		this.getStatement2().setDouble(name, value);
		
		this.replay();
		
		this.getStatement().setDouble(name, value);
		
		this.verify();
	}

	@DataProvider(name = "string-float")
	Object[][] stringFloatProvider()
	{
		return new Object[][] { new Object[] { "column", 1.0F } };
	}

	/**
	 * @see java.sql.CallableStatement#setFloat(java.lang.String, float)
	 */
	@Test(dataProvider = "string-float")
	public void setFloat(String name, float value) throws SQLException
	{
		this.getStatement1().setFloat(name, value);
		this.getStatement2().setFloat(name, value);
		
		this.replay();
		
		this.getStatement().setFloat(name, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.CallableStatement#setInt(java.lang.String, int)
	 */
	@Test(dataProvider = "string-int")
	public void setInt(String name, int value) throws SQLException
	{
		this.getStatement1().setInt(name, value);
		this.getStatement2().setInt(name, value);
		
		this.replay();
		
		this.getStatement().setInt(name, value);
		
		this.verify();
	}

	@DataProvider(name = "string-long")
	Object[][] stringLongProvider()
	{
		return new Object[][] { new Object[] { "column", 1L } };
	}

	/**
	 * @see java.sql.CallableStatement#setLong(java.lang.String, long)
	 */
	@Test(dataProvider = "string-long")
	public void setLong(String name, long value) throws SQLException
	{
		this.getStatement1().setLong(name, value);
		this.getStatement2().setLong(name, value);
		
		this.replay();
		
		this.getStatement().setLong(name, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.CallableStatement#setNull(java.lang.String, int)
	 */
	@Test(dataProvider = "string-int")
	public void setNull(String name, int sqlType) throws SQLException
	{
		this.getStatement1().setNull(name, sqlType);
		this.getStatement2().setNull(name, sqlType);
		
		this.replay();
		
		this.getStatement().setNull(name, sqlType);
		
		this.verify();
	}
	
	/**
	 * @see java.sql.CallableStatement#setNull(java.lang.String, int, java.lang.String)
	 */
	@Test(dataProvider = "string-int-string")
	public void setNull(String name, int sqlType, String typeName) throws SQLException
	{
		this.getStatement1().setNull(name, sqlType, typeName);
		this.getStatement2().setNull(name, sqlType, typeName);
		
		this.replay();
		
		this.getStatement().setNull(name, sqlType, typeName);
		
		this.verify();
	}

	@DataProvider(name = "string-object")
	Object[][] stringObjectProvider()
	{
		return new Object[][] { new Object[] { "column", new Object() } };
	}
	
	/**
	 * @see java.sql.CallableStatement#setObject(java.lang.String, java.lang.Object)
	 */
	@Test(dataProvider = "string-object")
	public void setObject(String name, Object value) throws SQLException
	{
		this.getStatement1().setObject(name, value);
		this.getStatement2().setObject(name, value);
		
		this.replay();
		
		this.getStatement().setObject(name, value);
		
		this.verify();
	}

	@DataProvider(name = "string-object-int")
	Object[][] stringObjectIntProvider()
	{
		return new Object[][] { new Object[] { "column", new Object(), Types.INTEGER } };
	}

	/**
	 * @see java.sql.CallableStatement#setObject(java.lang.String, java.lang.Object, int)
	 */
	@Test(dataProvider = "string-object-int")
	public void setObject(String name, Object value, int targetSqlType) throws SQLException
	{
		this.getStatement1().setObject(name, value, targetSqlType);
		this.getStatement2().setObject(name, value, targetSqlType);
		
		this.replay();
		
		this.getStatement().setObject(name, value, targetSqlType);
		
		this.verify();
	}

	@DataProvider(name = "string-object-int-int")
	Object[][] stringObjectIntIntProvider()
	{
		return new Object[][] { new Object[] { "column", new Object(), Types.INTEGER, 1 } };
	}

	/**
	 * @see java.sql.CallableStatement#setObject(java.lang.String, java.lang.Object, int, int)
	 */
	@Test(dataProvider = "string-object-int-int")
	public void setObject(String name, Object value, int targetSqlType, int scale) throws SQLException
	{
		this.getStatement1().setObject(name, value, targetSqlType, scale);
		this.getStatement2().setObject(name, value, targetSqlType, scale);
		
		this.replay();
		
		this.getStatement().setObject(name, value, targetSqlType, scale);
		
		this.verify();
	}

	@DataProvider(name = "string-short")
	Object[][] stringShortProvider()
	{
		return new Object[][] { new Object[] { "column", Integer.valueOf(1).shortValue() } };
	}

	/**
	 * @see java.sql.CallableStatement#setShort(java.lang.String, short)
	 */
	@Test(dataProvider = "string-short")
	public void setShort(String name, short value) throws SQLException
	{
		this.getStatement1().setShort(name, value);
		this.getStatement2().setShort(name, value);
		
		this.replay();
		
		this.getStatement().setShort(name, value);
		
		this.verify();
	}

	@DataProvider(name = "string-string")
	Object[][] stringStringProvider()
	{
		return new Object[][] { new Object[] { "column", "" } };
	}

	/**
	 * @see java.sql.CallableStatement#setString(java.lang.String, java.lang.String)
	 */
	@Test(dataProvider = "string-string")
	public void setString(String name, String value) throws SQLException
	{
		this.getStatement1().setString(name, value);
		this.getStatement2().setString(name, value);
		
		this.replay();
		
		this.getStatement().setString(name, value);
		
		this.verify();
	}

	@DataProvider(name = "string-time")
	Object[][] stringTimeProvider()
	{
		return new Object[][] { new Object[] { "column", new Time(System.currentTimeMillis()) } };
	}

	/**
	 * @see java.sql.CallableStatement#setTime(java.lang.String, java.sql.Time)
	 */
	@Test(dataProvider = "string-time")
	public void setTime(String name, Time value) throws SQLException
	{
		this.getStatement1().setTime(name, value);
		this.getStatement2().setTime(name, value);
		
		this.replay();
		
		this.getStatement().setTime(name, value);
		
		this.verify();
	}

	@DataProvider(name = "string-time-calendar")
	Object[][] stringTimeCalendarProvider()
	{
		return new Object[][] { new Object[] { "column", new Time(System.currentTimeMillis()), Calendar.getInstance() } };
	}

	/**
	 * @see java.sql.CallableStatement#setTime(java.lang.String, java.sql.Time, java.util.Calendar)
	 */
	@Test(dataProvider = "string-time-calendar")
	public void setTime(String name, Time value, Calendar calendar) throws SQLException
	{
		this.getStatement1().setTime(name, value, calendar);
		this.getStatement2().setTime(name, value, calendar);
		
		this.replay();
		
		this.getStatement().setTime(name, value, calendar);
		
		this.verify();
	}

	@DataProvider(name = "string-timestamp")
	Object[][] stringTimestampProvider()
	{
		return new Object[][] { new Object[] { "column", new Timestamp(System.currentTimeMillis()) } };
	}

	/**
	 * @see java.sql.CallableStatement#setTimestamp(java.lang.String, java.sql.Timestamp)
	 */
	@Test(dataProvider = "string-timestamp")
	public void setTimestamp(String name, Timestamp value) throws SQLException
	{
		this.getStatement1().setTimestamp(name, value);
		this.getStatement2().setTimestamp(name, value);
		
		this.replay();
		
		this.getStatement().setTimestamp(name, value);
		
		this.verify();
	}

	@DataProvider(name = "string-timestamp-calendar")
	Object[][] stringTimestampCalendarProvider()
	{
		return new Object[][] { new Object[] { "column", new Timestamp(System.currentTimeMillis()), Calendar.getInstance() } };
	}

	/**
	 * @see java.sql.CallableStatement#setTimestamp(java.lang.String, java.sql.Timestamp, java.util.Calendar)
	 */
	@Test(dataProvider = "string-timestamp-calendar")
	public void setTimestamp(String name, Timestamp value, Calendar calendar) throws SQLException
	{
		this.getStatement1().setTimestamp(name, value, calendar);
		this.getStatement2().setTimestamp(name, value, calendar);
		
		this.replay();
		
		this.getStatement().setTimestamp(name, value, calendar);
		
		this.verify();
	}

	@DataProvider(name = "string-url")
	Object[][] stringUrlProvider() throws MalformedURLException
	{
		return new Object[][] { new Object[] { "column", new URL("http://ha-jdbc.sf.net") } };
	}

	/**
	 * @see java.sql.CallableStatement#setURL(java.lang.String, java.net.URL)
	 */
	@Test(dataProvider = "string-url")
	public void setURL(String name, URL value) throws SQLException
	{
		this.getStatement1().setURL(name, value);
		this.getStatement2().setURL(name, value);
		
		this.replay();
		
		this.getStatement().setURL(name, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.CallableStatement#wasNull()
	 */
	@Test
	public boolean wasNull() throws SQLException
	{
		EasyMock.expect(this.getStatement1().wasNull()).andReturn(true);
		
		this.replay();
		
		boolean result = this.getStatement().wasNull();
		
		this.verify();
		
		assert result;
		
		return result;
	}

	/**
	 * @see java.sql.CallableStatement#getCharacterStream(int)
	 */
	@Test(dataProvider = "int")
	public Reader getCharacterStream(int index) throws SQLException
	{
		Reader reader = new StringReader("");
		
		EasyMock.expect(this.getStatement1().getCharacterStream(index)).andReturn(reader);
		
		this.replay();
		
		Reader result = this.getStatement().getCharacterStream(index);
		
		this.verify();
		
		assert result == reader;
		
		return result;
	}

	/**
	 * @see java.sql.CallableStatement#getCharacterStream(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public Reader getCharacterStream(String name) throws SQLException
	{
		Reader reader = new StringReader("");
		
		EasyMock.expect(this.getStatement1().getCharacterStream(name)).andReturn(reader);
		
		this.replay();
		
		Reader result = this.getStatement().getCharacterStream(name);
		
		this.verify();
		
		assert result == reader;
		
		return result;
	}

	/**
	 * @see java.sql.CallableStatement#getNCharacterStream(int)
	 */
	@Test(dataProvider = "int")
	public Reader getNCharacterStream(int index) throws SQLException
	{
		Reader reader = new StringReader("");
		
		EasyMock.expect(this.getStatement1().getNCharacterStream(index)).andReturn(reader);
		
		this.replay();
		
		Reader result = this.getStatement().getNCharacterStream(index);
		
		this.verify();
		
		assert result == reader;
		
		return result;
	}

	/**
	 * @see java.sql.CallableStatement#getNCharacterStream(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public Reader getNCharacterStream(String name) throws SQLException
	{
		Reader reader = new StringReader("");
		
		EasyMock.expect(this.getStatement1().getNCharacterStream(name)).andReturn(reader);
		
		this.replay();
		
		Reader result = this.getStatement().getNCharacterStream(name);
		
		this.verify();
		
		assert result == reader;
		
		return result;
	}

	/**
	 * @see java.sql.CallableStatement#getNClob(int)
	 */
	@Test(dataProvider = "int")
	public NClob getNClob(int index) throws SQLException
	{
		NClob clob = EasyMock.createMock(NClob.class);
		
		EasyMock.expect(this.getStatement1().getNClob(index)).andReturn(clob);
		
		this.replay();
		
		NClob result = this.getStatement().getNClob(index);
		
		this.verify();
		
		assert result == clob;
		
		return result;
	}

	/**
	 * @see java.sql.CallableStatement#getNClob(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public NClob getNClob(String name) throws SQLException
	{
		NClob clob = EasyMock.createMock(NClob.class);
		
		EasyMock.expect(this.getStatement1().getNClob(name)).andReturn(clob);
		
		this.replay();
		
		NClob result = this.getStatement().getNClob(name);
		
		this.verify();
		
		assert result == clob;
		
		return result;
	}

	/**
	 * @see java.sql.CallableStatement#getNString(int)
	 */
	@Test(dataProvider = "int")
	public String getNString(int index) throws SQLException
	{
		String string = "";
		
		EasyMock.expect(this.getStatement1().getNString(index)).andReturn(string);
		
		this.replay();
		
		String result = this.getStatement().getNString(index);
		
		this.verify();
		
		assert result == string;
		
		return result;
	}

	/**
	 * @see java.sql.CallableStatement#getNString(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public String getNString(String name) throws SQLException
	{
		String string = "";
		
		EasyMock.expect(this.getStatement1().getNString(name)).andReturn(string);
		
		this.replay();
		
		String result = this.getStatement().getNString(name);
		
		this.verify();
		
		assert result == string;
		
		return result;
	}

	/**
	 * @see java.sql.CallableStatement#getRowId(int)
	 */
	@Test(dataProvider = "int")
	public RowId getRowId(int index) throws SQLException
	{
		RowId rowId = EasyMock.createMock(RowId.class);
		
		EasyMock.expect(this.getStatement1().getRowId(index)).andReturn(rowId);
		
		this.replay();
		
		RowId result = this.getStatement().getRowId(index);
		
		this.verify();
		
		assert result == rowId;
		
		return result;
	}

	/**
	 * @see java.sql.CallableStatement#getRowId(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public RowId getRowId(String name) throws SQLException
	{
		RowId rowId = EasyMock.createMock(RowId.class);
		
		EasyMock.expect(this.getStatement1().getRowId(name)).andReturn(rowId);
		
		this.replay();
		
		RowId result = this.getStatement().getRowId(name);
		
		this.verify();
		
		assert result == rowId;
		
		return result;
	}

	/**
	 * @see java.sql.CallableStatement#getSQLXML(int)
	 */
	@Test(dataProvider = "int")
	public SQLXML getSQLXML(int index) throws SQLException
	{
		SQLXML xml = EasyMock.createMock(SQLXML.class);
		
		EasyMock.expect(this.getStatement1().getSQLXML(index)).andReturn(xml);
		
		this.replay();
		
		SQLXML result = this.getStatement().getSQLXML(index);
		
		this.verify();
		
		assert result == xml;
		
		return result;
	}

	/**
	 * @see java.sql.CallableStatement#getSQLXML(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public SQLXML getSQLXML(String name) throws SQLException
	{
		SQLXML xml = EasyMock.createMock(SQLXML.class);
		
		EasyMock.expect(this.getStatement1().getSQLXML(name)).andReturn(xml);
		
		this.replay();
		
		SQLXML result = this.getStatement().getSQLXML(name);
		
		this.verify();
		
		assert result == xml;
		
		return result;
	}

	@DataProvider(name = "string-inputStream")
	Object[][] stringInputStreamProvider()
	{
		return new Object[][] { new Object[] { "", new ByteArrayInputStream(new byte[0]) } };
	}

	/**
	 * @see java.sql.CallableStatement#setAsciiStream(java.lang.String, java.io.InputStream)
	 */
	@Test(dataProvider = "string-inputStream")
	public void setAsciiStream(String name, InputStream value) throws SQLException
	{
		File file = new File("");
		InputStream input1 = new ByteArrayInputStream(new byte[0]);
		InputStream input2 = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.getStatement1().setAsciiStream(name, input1);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.getStatement2().setAsciiStream(name, input2);
		
		this.replay();

		this.getStatement().setAsciiStream(name, value);
		
		this.verify();
	}

	@DataProvider(name = "string-inputStream-long")
	Object[][] stringInputStreamLongProvider()
	{
		return new Object[][] { new Object[] { "", new ByteArrayInputStream(new byte[0]), 1L } };
	}
		
	/**
	 * @see java.sql.CallableStatement#setAsciiStream(java.lang.String, java.io.InputStream, long)
	 */
	@Test(dataProvider = "string-inputStream-long")
	public void setAsciiStream(String name, InputStream value, long length) throws SQLException
	{
		File file = new File("");
		InputStream input1 = new ByteArrayInputStream(new byte[0]);
		InputStream input2 = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.getStatement1().setAsciiStream(name, input1, length);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.getStatement2().setAsciiStream(name, input2, length);
		
		this.replay();

		this.getStatement().setAsciiStream(name, value, length);
		
		this.verify();
	}

	/**
	 * @see java.sql.CallableStatement#setBinaryStream(java.lang.String, java.io.InputStream)
	 */
	@Test(dataProvider = "string-inputStream")
	public void setBinaryStream(String name, InputStream value) throws SQLException
	{
		File file = new File("");
		InputStream input1 = new ByteArrayInputStream(new byte[0]);
		InputStream input2 = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.getStatement1().setBinaryStream(name, input1);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.getStatement2().setBinaryStream(name, input2);
		
		this.replay();

		this.getStatement().setBinaryStream(name, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.CallableStatement#setBinaryStream(java.lang.String, java.io.InputStream, long)
	 */
	@Test(dataProvider = "string-inputStream-long")
	public void setBinaryStream(String name, InputStream value, long length) throws SQLException
	{
		File file = new File("");
		InputStream input1 = new ByteArrayInputStream(new byte[0]);
		InputStream input2 = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.getStatement1().setBinaryStream(name, input1, length);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.getStatement2().setBinaryStream(name, input2, length);
		
		this.replay();

		this.getStatement().setBinaryStream(name, value, length);
		
		this.verify();
	}

	@DataProvider(name = "string-blob")
	Object[][] stringBlobProvider() throws Exception
	{
		Map<Database, Blob> map = new TreeMap<Database, Blob>();
		
		map.put(this.database1, this.blob1);
		map.put(this.database2, this.blob2);
		
		EasyMock.expect(this.parent.getDatabaseCluster()).andReturn(this.cluster);
		
		this.parent.addChild(EasyMock.isA(BlobInvocationHandler.class));
		
		this.replay();
		
		Blob blob = ProxyFactory.createProxy(Blob.class, new BlobInvocationHandler(null, this.parent, null, map));
		
		this.verify();
		this.reset();
		
		return new Object[][] { new Object[] { "", new MockBlob() }, new Object[] { "", blob } };
	}

	/**
	 * @see java.sql.CallableStatement#setBlob(java.lang.String, java.sql.Blob)
	 */
	@Test(dataProvider = "string-blob")
	public void setBlob(String name, Blob value) throws SQLException
	{
		if (Proxy.isProxyClass(value.getClass()))
		{
			this.getStatement1().setBlob(name, this.blob1);
			this.getStatement2().setBlob(name, this.blob2);
		}
		else
		{
			this.getStatement1().setBlob(EasyMock.eq(name), EasyMock.isA(SerialBlob.class));
			this.getStatement2().setBlob(EasyMock.eq(name), EasyMock.isA(SerialBlob.class));
		}
		
		this.replay();

		this.getStatement().setBlob(name, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.CallableStatement#setBlob(java.lang.String, java.io.InputStream)
	 */
	@Test(dataProvider = "string-inputStream")
	public void setBlob(String name, InputStream value) throws SQLException
	{
		File file = new File("");
		InputStream input1 = new ByteArrayInputStream(new byte[0]);
		InputStream input2 = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.getStatement1().setBlob(name, input1);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.getStatement2().setBlob(name, input2);
		
		this.replay();

		this.getStatement().setBlob(name, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.CallableStatement#setBlob(java.lang.String, java.io.InputStream, long)
	 */
	@Test(dataProvider = "string-inputStream-long")
	public void setBlob(String name, InputStream value, long length) throws SQLException
	{
		File file = new File("");
		InputStream input1 = new ByteArrayInputStream(new byte[0]);
		InputStream input2 = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.getStatement1().setBlob(name, input1, length);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.getStatement2().setBlob(name, input2, length);
		
		this.replay();

		this.getStatement().setBlob(name, value, length);
		
		this.verify();
	}

	@DataProvider(name = "string-reader")
	Object[][] stringReaderProvider()
	{
		return new Object[][] { new Object[] { "", new StringReader("") } };
	}
	
	/**
	 * @see java.sql.CallableStatement#setCharacterStream(java.lang.String, java.io.Reader)
	 */
	@Test(dataProvider = "string-reader")
	public void setCharacterStream(String name, Reader value) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new CharArrayReader(new char[0]);
		Reader reader2 = new CharArrayReader(new char[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.getStatement1().setCharacterStream(name, reader1);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.getStatement2().setCharacterStream(name, reader2);
		
		this.replay();

		this.getStatement().setCharacterStream(name, value);
		
		this.verify();
	}

	@DataProvider(name = "string-reader-long")
	Object[][] stringReaderLongProvider()
	{
		return new Object[][] { new Object[] { "", new StringReader(""), 1L } };
	}

	/**
	 * @see java.sql.CallableStatement#setCharacterStream(java.lang.String, java.io.Reader, long)
	 */
	@Test(dataProvider = "string-reader-long")
	public void setCharacterStream(String name, Reader value, long length) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new CharArrayReader(new char[0]);
		Reader reader2 = new CharArrayReader(new char[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.getStatement1().setCharacterStream(name, reader1, length);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.getStatement2().setCharacterStream(name, reader2, length);
		
		this.replay();

		this.getStatement().setCharacterStream(name, value, length);
		
		this.verify();
	}

	@DataProvider(name = "string-clob")
	Object[][] stringClobProvider() throws Exception
	{
		Map<Database, Clob> map = new TreeMap<Database, Clob>();
		
		map.put(this.database1, this.clob1);
		map.put(this.database2, this.clob2);
		
		EasyMock.expect(this.parent.getDatabaseCluster()).andReturn(this.cluster);
		
		this.parent.addChild(EasyMock.isA(ClobInvocationHandler.class));

		this.replay();
		
		Clob clob = ProxyFactory.createProxy(Clob.class, new ClobInvocationHandler(null, this.parent, null, map));
		
		this.verify();
		this.reset();
		
		return new Object[][] { new Object[] { "", new MockClob() }, new Object[] { "", clob } };
	}

	/**
	 * @see java.sql.CallableStatement#setClob(java.lang.String, java.sql.Clob)
	 */
	@Test(dataProvider = "string-clob")
	public void setClob(String name, Clob value) throws SQLException
	{
		if (Proxy.isProxyClass(value.getClass()))
		{
			this.getStatement1().setClob(name, this.clob1);
			this.getStatement2().setClob(name, this.clob2);
		}
		else
		{
			this.getStatement1().setClob(EasyMock.eq(name), EasyMock.isA(SerialClob.class));
			this.getStatement2().setClob(EasyMock.eq(name), EasyMock.isA(SerialClob.class));
		}
		
		this.replay();

		this.getStatement().setClob(name, value);
		
		this.verify();
	}
	
	/**
	 * @see java.sql.CallableStatement#setClob(java.lang.String, java.io.Reader)
	 */
	@Test(dataProvider = "string-reader")
	public void setClob(String name, Reader value) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new CharArrayReader(new char[0]);
		Reader reader2 = new CharArrayReader(new char[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.getStatement1().setClob(name, reader1);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.getStatement2().setClob(name, reader2);
		
		this.replay();

		this.getStatement().setClob(name, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.CallableStatement#setClob(java.lang.String, java.io.Reader, long)
	 */
	@Test(dataProvider = "string-reader-long")
	public void setClob(String name, Reader value, long length) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new CharArrayReader(new char[0]);
		Reader reader2 = new CharArrayReader(new char[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.getStatement1().setClob(name, reader1, length);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.getStatement2().setClob(name, reader2, length);
		
		this.replay();

		this.getStatement().setClob(name, value, length);
		
		this.verify();
	}

	/**
	 * @see java.sql.CallableStatement#setNCharacterStream(java.lang.String, java.io.Reader)
	 */
	@Test(dataProvider = "string-reader")
	public void setNCharacterStream(String name, Reader value) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new CharArrayReader(new char[0]);
		Reader reader2 = new CharArrayReader(new char[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.getStatement1().setNCharacterStream(name, reader1);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.getStatement2().setNCharacterStream(name, reader2);
		
		this.replay();

		this.getStatement().setNCharacterStream(name, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.CallableStatement#setNCharacterStream(java.lang.String, java.io.Reader, long)
	 */
	@Test(dataProvider = "string-reader-long")
	public void setNCharacterStream(String name, Reader value, long length) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new CharArrayReader(new char[0]);
		Reader reader2 = new CharArrayReader(new char[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.getStatement1().setNCharacterStream(name, reader1, length);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.getStatement2().setNCharacterStream(name, reader2, length);
		
		this.replay();

		this.getStatement().setNCharacterStream(name, value, length);
		
		this.verify();
	}

	@DataProvider(name = "string-nclob")
	Object[][] stringNClobProvider() throws Exception
	{
		Map<Database, NClob> map = new TreeMap<Database, NClob>();
		
		map.put(this.database1, this.nClob1);
		map.put(this.database2, this.nClob2);
		
		EasyMock.expect(this.parent.getDatabaseCluster()).andReturn(this.cluster);
		
		this.parent.addChild(EasyMock.isA(ClobInvocationHandler.class));
		
		this.replay();
		
		NClob nClob = ProxyFactory.createProxy(NClob.class, new ClobInvocationHandler(null, this.parent, null, map));
		
		this.verify();
		this.reset();
		
		return new Object[][] { new Object[] { "", new MockClob() }, new Object[] { "", nClob } };
	}

	/**
	 * @see java.sql.CallableStatement#setNClob(java.lang.String, java.sql.NClob)
	 */
	@Test(dataProvider = "string-nclob")
	public void setNClob(String name, NClob value) throws SQLException
	{
		if (Proxy.isProxyClass(value.getClass()))
		{
			this.getStatement1().setNClob(name, this.nClob1);
			this.getStatement2().setNClob(name, this.nClob2);
		}
		else
		{
			this.getStatement1().setNClob(EasyMock.eq(name), EasyMock.isA(NClob.class));
			this.getStatement2().setNClob(EasyMock.eq(name), EasyMock.isA(NClob.class));
		}
		
		this.replay();

		this.getStatement().setNClob(name, value);
		
		this.verify();
	}
	
	/**
	 * @see java.sql.CallableStatement#setNClob(java.lang.String, java.io.Reader)
	 */
	@Test(dataProvider = "string-reader")
	public void setNClob(String name, Reader value) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new CharArrayReader(new char[0]);
		Reader reader2 = new CharArrayReader(new char[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.getStatement1().setNClob(name, reader1);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.getStatement2().setNClob(name, reader2);
		
		this.replay();

		this.getStatement().setNClob(name, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.CallableStatement#setNClob(java.lang.String, java.io.Reader, long)
	 */
	@Test(dataProvider = "string-reader-long")
	public void setNClob(String name, Reader value, long length) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new CharArrayReader(new char[0]);
		Reader reader2 = new CharArrayReader(new char[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.getStatement1().setNClob(name, reader1, length);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.getStatement2().setNClob(name, reader2, length);
		
		this.replay();

		this.getStatement().setNClob(name, value, length);
		
		this.verify();
	}

	/**
	 * @see java.sql.CallableStatement#setNString(java.lang.String, java.lang.String)
	 */
	@Test(dataProvider = "string-string")
	public void setNString(String name, String value) throws SQLException
	{
		this.getStatement1().setNString(name, value);
		this.getStatement2().setNString(name, value);
		
		this.replay();

		this.getStatement().setNString(name, value);
		
		this.verify();
	}

	@DataProvider(name = "string-rowId")
	Object[][] stringRowIdProvider()
	{
		return new Object[][] { new Object[] { "", EasyMock.createMock(RowId.class) } };
	}

	/**
	 * @see java.sql.CallableStatement#setRowId(java.lang.String, java.sql.RowId)
	 */
	@Test(dataProvider = "string-rowId")
	public void setRowId(String name, RowId value) throws SQLException
	{
		this.getStatement1().setRowId(name, value);
		this.getStatement2().setRowId(name, value);
		
		this.replay();

		this.getStatement().setRowId(name, value);
		
		this.verify();
	}

	@DataProvider(name = "string-xml")
	Object[][] stringSQLXMLProvider()
	{
		return new Object[][] { new Object[] { "", EasyMock.createMock(SQLXML.class) } };
	}

	/**
	 * @see java.sql.CallableStatement#setSQLXML(java.lang.String, java.sql.SQLXML)
	 */
	@Test(dataProvider = "string-xml")
	public void setSQLXML(String name, SQLXML value) throws SQLException
	{
		this.getStatement1().setSQLXML(name, value);
		this.getStatement2().setSQLXML(name, value);
		
		this.replay();

		this.getStatement().setSQLXML(name, value);
		
		this.verify();
	}
}
