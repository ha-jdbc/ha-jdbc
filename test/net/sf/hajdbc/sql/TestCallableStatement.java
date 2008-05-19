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
@Test
@SuppressWarnings({ "unchecked", "nls" })
public class TestCallableStatement extends AbstractTestPreparedStatement<CallableStatement> implements java.sql.CallableStatement
{
	@Override
	protected Class<CallableStatement> getStatementClass()
	{
		return java.sql.CallableStatement.class;
	}
	
	@Override
	protected AbstractStatementInvocationHandler<?, CallableStatement> getInvocationHandler(Map<Database, CallableStatement> map) throws Exception
	{
		return new CallableStatementInvocationHandler(this.connection, this.parent, EasyMock.createMock(Invoker.class), map, this.transactionContext, this.fileSupport);
	}
	
	/**
	 * @see net.sf.hajdbc.sql.AbstractTestStatement#recordConstructor()
	 */
	@Override
	protected void recordConstructor() throws SQLException
	{
		this.parent.addChild(EasyMock.isA(CallableStatementInvocationHandler.class));
	}
	
	@Test(dataProvider = "int")
	public void testGetArray(int index) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		Array array = EasyMock.createMock(Array.class);
		
		EasyMock.expect(this.statement1.getArray(index)).andReturn(array);
		
		this.replay();
		
		Array value = this.getArray(index);
		
		this.verify();
		
		assert value == array;
	}
	
	/**
	 * @see java.sql.CallableStatement#getArray(int)
	 */
	@Override
	public Array getArray(int index) throws SQLException
	{
		return this.statement.getArray(index);
	}

	@Test(dataProvider = "string")
	public void testGetArray(String name) throws SQLException
	{
		Array array = EasyMock.createMock(Array.class);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getArray(name)).andReturn(array);
		
		this.replay();
		
		Array value = this.getArray(name);
		
		this.verify();
		
		assert value == array;
	}
	
	/**
	 * @see java.sql.CallableStatement#getArray(java.lang.String)
	 */
	@Override
	public Array getArray(String name) throws SQLException
	{
		return this.statement.getArray(name);
	}

	@Test(dataProvider = "int")
	public void testGetBigDecimal(int index) throws SQLException
	{
		BigDecimal decimal = new BigDecimal(10);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getBigDecimal(index)).andReturn(decimal);
		
		this.replay();
		
		BigDecimal value = this.getBigDecimal(index);
		
		this.verify();
		
		assert value == decimal;
	}
	
	/**
	 * @see java.sql.CallableStatement#getBigDecimal(int)
	 */
	@Override
	public BigDecimal getBigDecimal(int index) throws SQLException
	{
		return this.statement.getBigDecimal(index);
	}

	@Test(dataProvider = "int-int")
	@Deprecated
	public void testGetBigDecimal(int index, int scale) throws SQLException
	{
		BigDecimal decimal = new BigDecimal(10);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getBigDecimal(index, scale)).andReturn(decimal);
		
		this.replay();
		
		BigDecimal value = this.getBigDecimal(index, scale);
		
		this.verify();
		
		assert value == decimal;
	}
	
	/**
	 * @see java.sql.CallableStatement#getBigDecimal(int, int)
	 */
	@SuppressWarnings("deprecation")
	@Override
	@Deprecated
	public BigDecimal getBigDecimal(int index, int scale) throws SQLException
	{
		return this.statement.getBigDecimal(index, scale);
	}

	@Test(dataProvider = "string")
	public void testGetBigDecimal(String name) throws SQLException
	{
		BigDecimal decimal = new BigDecimal(10);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getBigDecimal(name)).andReturn(decimal);
		
		this.replay();
		
		BigDecimal value = this.getBigDecimal(name);
		
		this.verify();
		
		assert value == decimal;
	}
	
	/**
	 * @see java.sql.CallableStatement#getBigDecimal(java.lang.String)
	 */
	@Override
	public BigDecimal getBigDecimal(String name) throws SQLException
	{
		return this.statement.getBigDecimal(name);
	}

	@Test(dataProvider = "int")
	public void testGetBlob(int index) throws SQLException
	{
		Blob blob = EasyMock.createMock(Blob.class);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getBlob(index)).andReturn(blob);
		
		this.replay();
		
		Blob value = this.getBlob(index);
		
		this.verify();
		
		assert value == blob;
	}
	
	/**
	 * @see java.sql.CallableStatement#getBlob(int)
	 */
	@Override
	public Blob getBlob(int index) throws SQLException
	{
		return this.statement.getBlob(index);
	}

	@Test(dataProvider = "string")
	public void testGetBlog(String name) throws SQLException
	{
		Blob blob = EasyMock.createMock(Blob.class);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getBlob(name)).andReturn(blob);
		
		this.replay();
		
		Blob value = this.statement.getBlob(name);
		
		this.verify();
		
		assert value == blob;
	}
	
	/**
	 * @see java.sql.CallableStatement#getBlob(java.lang.String)
	 */
	@Override
	public Blob getBlob(String name) throws SQLException
	{
		return this.statement.getBlob(name);
	}

	@Test(dataProvider = "int")
	public void testGetBoolean(int index) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getBoolean(index)).andReturn(true);
		
		this.replay();
		
		boolean value = this.getBoolean(index);
		
		this.verify();
		
		assert value;
	}
	
	/**
	 * @see java.sql.CallableStatement#getBoolean(int)
	 */
	@Override
	public boolean getBoolean(int index) throws SQLException
	{
		return this.statement.getBoolean(index);
	}

	@Test(dataProvider = "string")
	public void testGetBoolean(String name) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getBoolean(name)).andReturn(true);
		
		this.replay();
		
		boolean value = this.getBoolean(name);
		
		this.verify();
		
		assert value;
	}
	
	/**
	 * @see java.sql.CallableStatement#getBoolean(java.lang.String)
	 */
	@Override
	public boolean getBoolean(String name) throws SQLException
	{
		return this.statement.getBoolean(name);
	}

	@Test(dataProvider = "int")
	public void testGetByte(int index) throws SQLException
	{
		byte b = Integer.valueOf(1).byteValue();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getByte(index)).andReturn(b);
		
		this.replay();
		
		byte value = this.getByte(index);
		
		this.verify();
		
		assert value == b;
	}
	
	/**
	 * @see java.sql.CallableStatement#getByte(int)
	 */
	@Override
	public byte getByte(int index) throws SQLException
	{
		return this.statement.getByte(index);
	}

	@Test(dataProvider = "string")
	public void testGetByte(String name) throws SQLException
	{
		byte b = Integer.valueOf(1).byteValue();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getByte(name)).andReturn(b);
		
		this.replay();
		
		byte value = this.getByte(name);
		
		this.verify();
		
		assert value == b;
	}
	
	/**
	 * @see java.sql.CallableStatement#getByte(java.lang.String)
	 */
	@Override
	public byte getByte(String name) throws SQLException
	{
		return this.statement.getByte(name);
	}

	@Test(dataProvider = "int")
	public void testGetBytes(int index) throws SQLException
	{
		byte[] bytes = new byte[0];
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getBytes(index)).andReturn(bytes);
		
		this.replay();
		
		byte[] value = this.getBytes(index);
		
		this.verify();
		
		assert value == bytes;
	}
	
	/**
	 * @see java.sql.CallableStatement#getBytes(int)
	 */
	@Override
	public byte[] getBytes(int index) throws SQLException
	{
		return this.statement.getBytes(index);
	}

	@Test(dataProvider = "string")
	public void testGetBytes(String name) throws SQLException
	{
		byte[] bytes = new byte[0];
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getBytes(name)).andReturn(bytes);
		
		this.replay();
		
		byte[] value = this.getBytes(name);
		
		this.verify();
		
		assert value == bytes;
	}
	
	/**
	 * @see java.sql.CallableStatement#getBytes(java.lang.String)
	 */
	@Override
	public byte[] getBytes(String name) throws SQLException
	{
		return this.statement.getBytes(name);
	}

	@Test(dataProvider = "int")
	public void testGetClob(int index) throws SQLException
	{
		Clob clob = EasyMock.createMock(Clob.class);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getClob(index)).andReturn(clob);
		
		this.replay();
		
		Clob value = this.getClob(index);
		
		this.verify();
		
		assert value == clob;
	}
	
	/**
	 * @see java.sql.CallableStatement#getClob(int)
	 */
	@Override
	public Clob getClob(int index) throws SQLException
	{
		return this.statement.getClob(index);
	}

	@Test(dataProvider = "string")
	public void testGetClob(String name) throws SQLException
	{
		Clob clob = EasyMock.createMock(Clob.class);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getClob(name)).andReturn(clob);
		
		this.replay();
		
		Clob value = this.getClob(name);
		
		this.verify();
		
		assert value == clob;
	}
	
	/**
	 * @see java.sql.CallableStatement#getClob(java.lang.String)
	 */
	@Override
	public Clob getClob(String name) throws SQLException
	{
		return this.statement.getClob(name);
	}

	@Test(dataProvider = "int")
	public void testGetDate(int index) throws SQLException
	{
		Date date = new Date(System.currentTimeMillis());
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getDate(index)).andReturn(date);
		
		this.replay();
		
		Date value = this.getDate(index);
		
		this.verify();
		
		assert value == date;
	}
	
	/**
	 * @see java.sql.CallableStatement#getDate(int)
	 */
	@Override
	public Date getDate(int index) throws SQLException
	{
		return this.statement.getDate(index);
	}

	@DataProvider(name = "int-calendar")
	Object[][] intCalendarProvider()
	{
		return new Object[][] { new Object[] { 1, Calendar.getInstance() } };
	}

	@Test(dataProvider = "int-calendar")
	public void testGetDate(int index, Calendar calendar) throws SQLException
	{
		Date date = new Date(System.currentTimeMillis());
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getDate(index, calendar)).andReturn(date);
		
		this.replay();
		
		Date value = this.getDate(index, calendar);
		
		this.verify();
		
		assert value == date;
	}
	
	/**
	 * @see java.sql.CallableStatement#getDate(int, java.util.Calendar)
	 */
	@Override
	public Date getDate(int index, Calendar calendar) throws SQLException
	{
		return this.statement.getDate(index, calendar);
	}

	@Test(dataProvider = "string")
	public void testGetDate(String name) throws SQLException
	{
		Date date = new Date(System.currentTimeMillis());
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getDate(name)).andReturn(date);
		
		this.replay();
		
		Date value = this.getDate(name);
		
		this.verify();
		
		assert value == date;
	}
	
	/**
	 * @see java.sql.CallableStatement#getDate(java.lang.String)
	 */
	@Override
	public Date getDate(String name) throws SQLException
	{
		return this.statement.getDate(name);
	}

	@DataProvider(name = "string-calendar")
	Object[][] stringCalendarProvider()
	{
		return new Object[][] { new Object[] { "column", Calendar.getInstance() } };
	}

	@Test(dataProvider = "string-calendar")
	public void testGetDate(String name, Calendar calendar) throws SQLException
	{
		Date date = new Date(System.currentTimeMillis());
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getDate(name, calendar)).andReturn(date);
		
		this.replay();
		
		Date value = this.getDate(name, calendar);
		
		this.verify();
		
		assert value == date;
	}
	
	/**
	 * @see java.sql.CallableStatement#getDate(java.lang.String, java.util.Calendar)
	 */
	@Override
	public Date getDate(String name, Calendar calendar) throws SQLException
	{
		return this.statement.getDate(name, calendar);
	}

	@Test(dataProvider = "int")
	public void testGetDouble(int index) throws SQLException
	{
		double d = 1.0;
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getDouble(index)).andReturn(d);
		
		this.replay();
		
		double value = this.getDouble(index);
		
		this.verify();
		
		assert value == d;
	}
	
	/**
	 * @see java.sql.CallableStatement#getDouble(int)
	 */
	@Override
	public double getDouble(int index) throws SQLException
	{
		return this.statement.getDouble(index);
	}

	@Test(dataProvider = "string")
	public void testGetDouble(String name) throws SQLException
	{
		double d = 1.0;
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getDouble(name)).andReturn(d);
		
		this.replay();
		
		double value = this.getDouble(name);
		
		this.verify();
		
		assert value == d;
	}
	
	/**
	 * @see java.sql.CallableStatement#getDouble(java.lang.String)
	 */
	@Override
	public double getDouble(String name) throws SQLException
	{
		return this.statement.getDouble(name);
	}

	@Test(dataProvider = "int")
	public void testGetFloat(int index) throws SQLException
	{
		float f = 1.0f;
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getFloat(index)).andReturn(f);
		
		this.replay();
		
		float value = this.getFloat(index);
		
		this.verify();
		
		assert value == f;
	}
	
	/**
	 * @see java.sql.CallableStatement#getFloat(int)
	 */
	@Override
	public float getFloat(int index) throws SQLException
	{
		return this.statement.getFloat(index);
	}

	@Test(dataProvider = "string")
	public void testGetFloat(String name) throws SQLException
	{
		float f = 1.0f;
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getFloat(name)).andReturn(f);
		
		this.replay();
		
		float value = this.getFloat(name);
		
		this.verify();
		
		assert value == f;
	}
	
	/**
	 * @see java.sql.CallableStatement#getFloat(java.lang.String)
	 */
	@Override
	public float getFloat(String name) throws SQLException
	{
		return this.statement.getFloat(name);
	}

	@Test(dataProvider = "int")
	public void testGetInt(int index) throws SQLException
	{
		int i = 1;
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getInt(index)).andReturn(i);
		
		this.replay();
		
		int value = this.getInt(index);
		
		this.verify();
		
		assert value == i;
	}
	
	/**
	 * @see java.sql.CallableStatement#getInt(int)
	 */
	@Override
	public int getInt(int index) throws SQLException
	{
		return this.statement.getInt(index);
	}

	@Test(dataProvider = "string")
	public void testGetInt(String name) throws SQLException
	{
		int i = 1;
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getInt(name)).andReturn(i);
		
		this.replay();
		
		int value = this.getInt(name);
		
		this.verify();
		
		assert value == i;
	}
	
	/**
	 * @see java.sql.CallableStatement#getInt(java.lang.String)
	 */
	@Override
	public int getInt(String name) throws SQLException
	{
		return this.statement.getInt(name);
	}

	@Test(dataProvider = "int")
	public void testGetLong(int index) throws SQLException
	{
		long i = 1;
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getLong(index)).andReturn(i);
		
		this.replay();
		
		long value = this.getLong(index);
		
		this.verify();
		
		assert value == i;
	}
	
	/**
	 * @see java.sql.CallableStatement#getLong(int)
	 */
	@Override
	public long getLong(int index) throws SQLException
	{
		return this.statement.getLong(index);
	}

	@Test(dataProvider = "string")
	public void testGetLong(String name) throws SQLException
	{
		long i = 1;
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getLong(name)).andReturn(i);
		
		this.replay();
		
		long value = this.statement.getLong(name);
		
		this.verify();
		
		assert value == i;
	}
	
	/**
	 * @see java.sql.CallableStatement#getLong(java.lang.String)
	 */
	@Override
	public long getLong(String name) throws SQLException
	{
		return this.statement.getLong(name);
	}

	@Test(dataProvider = "int")
	public void testGetObject(int index) throws SQLException
	{
		Object object = new Object();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getObject(index)).andReturn(object);
		
		this.replay();
		
		Object value = this.getObject(index);
		
		this.verify();
		
		assert value == object;
	}
	
	/**
	 * @see java.sql.CallableStatement#getObject(int)
	 */
	@Override
	public Object getObject(int index) throws SQLException
	{
		return this.statement.getObject(index);
	}

	@DataProvider(name = "int-map")
	Object[][] intMapProvider()
	{
		return new Object[][] { new Object[] { 1, Collections.EMPTY_MAP } };
	}
	
	@Test(dataProvider = "int-map")
	public void testGetObject(int index, Map<String, Class<?>> map) throws SQLException
	{
		Object object = new Object();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getObject(index, map)).andReturn(object);
		
		this.replay();
		
		Object value = this.getObject(index, map);
		
		this.verify();
		
		assert value == object;
	}
	
	/**
	 * @see java.sql.CallableStatement#getObject(int, java.util.Map)
	 */
	@Override
	public Object getObject(int index, Map<String, Class<?>> map) throws SQLException
	{
		return this.statement.getObject(index, map);
	}

	@Test(dataProvider = "string")
	public void testGetObject(String name) throws SQLException
	{
		Object object = new Object();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getObject(name)).andReturn(object);
		
		this.replay();
		
		Object value = this.getObject(name);
		
		this.verify();
		
		assert value == object;
	}
	
	/**
	 * @see java.sql.CallableStatement#getObject(java.lang.String)
	 */
	@Override
	public Object getObject(String name) throws SQLException
	{
		return this.statement.getObject(name);
	}

	@DataProvider(name = "string-map")
	Object[][] stringMapProvider()
	{
		return new Object[][] { new Object[] { "column", Collections.EMPTY_MAP } };
	}

	@Test(dataProvider = "string-map")
	public void testGetObject(String name, Map<String, Class<?>> map) throws SQLException
	{
		Object object = new Object();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getObject(name, map)).andReturn(object);
		
		this.replay();
		
		Object value = this.getObject(name, map);
		
		this.verify();
		
		assert value == object;
	}
	
	/**
	 * @see java.sql.CallableStatement#getObject(java.lang.String, java.util.Map)
	 */
	@Override
	public Object getObject(String name, Map<String, Class<?>> map) throws SQLException
	{
		return this.statement.getObject(name, map);
	}

	@Test(dataProvider = "int")
	public void testGetRef(int index) throws SQLException
	{
		Ref ref = EasyMock.createMock(Ref.class);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getRef(index)).andReturn(ref);
		
		this.replay();
		
		Ref value = this.getRef(index);
		
		this.verify();
		
		assert value == ref;
	}
	
	/**
	 * @see java.sql.CallableStatement#getRef(int)
	 */
	@Override
	public Ref getRef(int index) throws SQLException
	{
		return this.statement.getRef(index);
	}

	@Test(dataProvider = "string")
	public void testGetRef(String name) throws SQLException
	{
		Ref ref = EasyMock.createMock(Ref.class);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getRef(name)).andReturn(ref);
		
		this.replay();
		
		Ref value = this.getRef(name);
		
		this.verify();
		
		assert value == ref;
	}
	
	/**
	 * @see java.sql.CallableStatement#getRef(java.lang.String)
	 */
	@Override
	public Ref getRef(String name) throws SQLException
	{
		return this.statement.getRef(name);
	}

	@Test(dataProvider = "int")
	public void testGetShort(int index) throws SQLException
	{
		short s = Integer.valueOf(1).shortValue();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getShort(index)).andReturn(s);
		
		this.replay();
		
		short value = this.getShort(index);
		
		this.verify();
		
		assert value == s;
	}
	
	/**
	 * @see java.sql.CallableStatement#getShort(int)
	 */
	@Override
	public short getShort(int index) throws SQLException
	{
		return this.statement.getShort(index);
	}

	@Test(dataProvider = "string")
	public void testGetShort(String name) throws SQLException
	{
		short s = Integer.valueOf(1).shortValue();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getShort(name)).andReturn(s);
		
		this.replay();
		
		short value = this.getShort(name);
		
		this.verify();
		
		assert value == s;
	}
	
	/**
	 * @see java.sql.CallableStatement#getShort(java.lang.String)
	 */
	@Override
	public short getShort(String name) throws SQLException
	{
		return this.statement.getShort(name);
	}

	@Test(dataProvider = "int")
	public void testGetString(int index) throws SQLException
	{
		String s = "";
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getString(index)).andReturn(s);
		
		this.replay();
		
		String value = this.getString(index);
		
		this.verify();
		
		assert value == s;
	}
	
	/**
	 * @see java.sql.CallableStatement#getString(int)
	 */
	@Override
	public String getString(int index) throws SQLException
	{
		return this.statement.getString(index);
	}

	@Test(dataProvider = "string")
	public void testGetString(String name) throws SQLException
	{
		String s = "";
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getString(name)).andReturn(s);
		
		this.replay();
		
		String value = this.getString(name);
		
		this.verify();
		
		assert value == s;
	}
	
	/**
	 * @see java.sql.CallableStatement#getString(java.lang.String)
	 */
	@Override
	public String getString(String name) throws SQLException
	{
		return this.statement.getString(name);
	}

	@Test(dataProvider = "int")
	public void testGetTime(int index) throws SQLException
	{
		Time time = new Time(System.currentTimeMillis());
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getTime(index)).andReturn(time);
		
		this.replay();
		
		Time value = this.getTime(index);
		
		this.verify();
		
		assert value == time;
	}
	
	/**
	 * @see java.sql.CallableStatement#getTime(int)
	 */
	@Override
	public Time getTime(int index) throws SQLException
	{
		return this.statement.getTime(index);
	}

	@Test(dataProvider = "int-calendar")
	public void testGetTime(int index, Calendar calendar) throws SQLException
	{
		Time time = new Time(System.currentTimeMillis());
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getTime(index, calendar)).andReturn(time);
		
		this.replay();
		
		Time value = this.getTime(index, calendar);
		
		this.verify();
		
		assert value == time;
	}
	
	/**
	 * @see java.sql.CallableStatement#getTime(int, java.util.Calendar)
	 */
	@Override
	public Time getTime(int index, Calendar calendar) throws SQLException
	{
		return this.statement.getTime(index, calendar);
	}

	@Test(dataProvider = "string")
	public void testGetTime(String name) throws SQLException
	{
		Time time = new Time(System.currentTimeMillis());
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getTime(name)).andReturn(time);
		
		this.replay();
		
		Time value = this.getTime(name);
		
		this.verify();
		
		assert value == time;
	}
	
	/**
	 * @see java.sql.CallableStatement#getTime(java.lang.String)
	 */
	@Override
	public Time getTime(String name) throws SQLException
	{
		return this.statement.getTime(name);
	}

	@Test(dataProvider = "string-calendar")
	public void testGetTime(String name, Calendar calendar) throws SQLException
	{
		Time time = new Time(System.currentTimeMillis());
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getTime(name, calendar)).andReturn(time);
		
		this.replay();
		
		Time value = this.getTime(name, calendar);
		
		this.verify();
		
		assert value == time;
	}
	
	/**
	 * @see java.sql.CallableStatement#getTime(java.lang.String, java.util.Calendar)
	 */
	@Override
	public Time getTime(String name, Calendar calendar) throws SQLException
	{
		return this.statement.getTime(name, calendar);
	}

	@Test(dataProvider = "int")
	public void testGetTimestamp(int index) throws SQLException
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getTimestamp(index)).andReturn(timestamp);
		
		this.replay();
		
		Timestamp value = this.getTimestamp(index);
		
		this.verify();
		
		assert value == timestamp;
	}
	
	/**
	 * @see java.sql.CallableStatement#getTimestamp(int)
	 */
	@Override
	public Timestamp getTimestamp(int index) throws SQLException
	{
		return this.statement.getTimestamp(index);
	}

	@Test(dataProvider = "int-calendar")
	public void testGetTimestamp(int index, Calendar calendar) throws SQLException
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getTimestamp(index, calendar)).andReturn(timestamp);
		
		this.replay();
		
		Timestamp value = this.getTimestamp(index, calendar);
		
		this.verify();
		
		assert value == timestamp;
	}
	
	/**
	 * @see java.sql.CallableStatement#getTimestamp(int, java.util.Calendar)
	 */
	@Override
	public Timestamp getTimestamp(int index, Calendar calendar) throws SQLException
	{
		return this.statement.getTimestamp(index, calendar);
	}

	@Test(dataProvider = "string")
	public void testGetTimestamp(String name) throws SQLException
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getTimestamp(name)).andReturn(timestamp);
		
		this.replay();
		
		Timestamp value = this.getTimestamp(name);
		
		this.verify();
		
		assert value == timestamp;
	}
	
	/**
	 * @see java.sql.CallableStatement#getTimestamp(java.lang.String)
	 */
	@Override
	public Timestamp getTimestamp(String name) throws SQLException
	{
		return this.statement.getTimestamp(name);
	}

	@Test(dataProvider = "string-calendar")
	public void testGetTimestamp(String name, Calendar calendar) throws SQLException
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getTimestamp(name, calendar)).andReturn(timestamp);
		
		this.replay();
		
		Timestamp value = this.getTimestamp(name, calendar);
		
		this.verify();
		
		assert value == timestamp;
	}
	
	/**
	 * @see java.sql.CallableStatement#getTimestamp(java.lang.String, java.util.Calendar)
	 */
	@Override
	public Timestamp getTimestamp(String name, Calendar calendar) throws SQLException
	{
		return this.statement.getTimestamp(name, calendar);
	}

	@Test(dataProvider = "int")
	public void testGetURL(int index) throws SQLException
	{
		try
		{
			URL url = new URL("http://ha-jdbc.sf.net");
			
			EasyMock.expect(this.cluster.isActive()).andReturn(true);
			
			EasyMock.expect(this.statement1.getURL(index)).andReturn(url);
			
			this.replay();
			
			URL value = this.getURL(index);
			
			this.verify();
			
			assert value == url;
		}
		catch (MalformedURLException e)
		{
			assert false : e;
		}
	}
	
	/**
	 * @see java.sql.CallableStatement#getURL(int)
	 */
	@Override
	public URL getURL(int index) throws SQLException
	{
		return this.statement.getURL(index);
	}

	@Test(dataProvider = "string")
	public void testGetURL(String name) throws SQLException
	{
		try
		{
			URL url = new URL("http://ha-jdbc.sf.net");
			
			EasyMock.expect(this.cluster.isActive()).andReturn(true);
			
			EasyMock.expect(this.statement1.getURL(name)).andReturn(url);
			
			this.replay();
			
			URL value = this.getURL(name);
			
			this.verify();
			
			assert value == url;
		}
		catch (MalformedURLException e)
		{
			assert false : e;
		}
	}
	
	/**
	 * @see java.sql.CallableStatement#getURL(java.lang.String)
	 */
	@Override
	public URL getURL(String name) throws SQLException
	{
		return this.statement.getURL(name);
	}

	/**
	 * @see java.sql.CallableStatement#registerOutParameter(int, int)
	 */
	@Test(dataProvider = "int-int")
	public void registerOutParameter(int index, int sqlType) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.registerOutParameter(index, sqlType);
		this.statement2.registerOutParameter(index, sqlType);
		
		this.replay();
		
		this.statement.registerOutParameter(index, sqlType);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.registerOutParameter(index, sqlType, scale);
		this.statement2.registerOutParameter(index, sqlType, scale);
		
		this.replay();
		
		this.statement.registerOutParameter(index, sqlType, scale);
		
		this.verify();
	}
	
	/**
	 * @see java.sql.CallableStatement#registerOutParameter(int, int, java.lang.String)
	 */
	@Test(dataProvider = "int-int-string")
	public void registerOutParameter(int index, int sqlType, String typeName) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.registerOutParameter(index, sqlType, typeName);
		this.statement2.registerOutParameter(index, sqlType, typeName);
		
		this.replay();
		
		this.statement.registerOutParameter(index, sqlType, typeName);
		
		this.verify();
	}
	
	/**
	 * @see java.sql.CallableStatement#registerOutParameter(java.lang.String, int)
	 */
	@Test(dataProvider = "string-int")
	public void registerOutParameter(String name, int sqlType) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.registerOutParameter(name, sqlType);
		this.statement2.registerOutParameter(name, sqlType);
		
		this.replay();
		
		this.statement.registerOutParameter(name, sqlType);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.registerOutParameter(name, sqlType, scale);
		this.statement2.registerOutParameter(name, sqlType, scale);
		
		this.replay();
		
		this.statement.registerOutParameter(name, sqlType, scale);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.registerOutParameter(name, sqlType, typeName);
		this.statement2.registerOutParameter(name, sqlType, typeName);
		
		this.replay();
		
		this.statement.registerOutParameter(name, sqlType, typeName);
		
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
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(inputStream1);
		
		this.statement1.setAsciiStream(name, inputStream1, length);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(inputStream2);
		
		this.statement2.setAsciiStream(name, inputStream2, length);
		
		this.replay();
		
		this.statement.setAsciiStream(name, value, length);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setBigDecimal(name, value);
		this.statement2.setBigDecimal(name, value);
		
		this.replay();
		
		this.statement.setBigDecimal(name, value);
		
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
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(inputStream1);
		
		this.statement1.setBinaryStream(name, inputStream1, length);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(inputStream2);
		
		this.statement2.setBinaryStream(name, inputStream2, length);
		
		this.replay();
		
		this.statement.setBinaryStream(name, value, length);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setBoolean(name, value);
		this.statement2.setBoolean(name, value);
		
		this.replay();
		
		this.statement.setBoolean(name, value);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setByte(name, value);
		this.statement2.setByte(name, value);
		
		this.replay();
		
		this.statement.setByte(name, value);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setBytes(name, value);
		this.statement2.setBytes(name, value);
		
		this.replay();
		
		this.statement.setBytes(name, value);
		
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
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.statement1.setCharacterStream(name, reader1, length);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.statement2.setCharacterStream(name, reader2, length);
		
		this.replay();
		
		this.statement.setCharacterStream(name, value, length);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setDate(name, value);
		this.statement2.setDate(name, value);
		
		this.replay();
		
		this.statement.setDate(name, value);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setDate(name, value, calendar);
		this.statement2.setDate(name, value, calendar);
		
		this.replay();
		
		this.statement.setDate(name, value, calendar);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setDouble(name, value);
		this.statement2.setDouble(name, value);
		
		this.replay();
		
		this.statement.setDouble(name, value);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setFloat(name, value);
		this.statement2.setFloat(name, value);
		
		this.replay();
		
		this.statement.setFloat(name, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.CallableStatement#setInt(java.lang.String, int)
	 */
	@Test(dataProvider = "string-int")
	public void setInt(String name, int value) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setInt(name, value);
		this.statement2.setInt(name, value);
		
		this.replay();
		
		this.statement.setInt(name, value);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setLong(name, value);
		this.statement2.setLong(name, value);
		
		this.replay();
		
		this.statement.setLong(name, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.CallableStatement#setNull(java.lang.String, int)
	 */
	@Test(dataProvider = "string-int")
	public void setNull(String name, int sqlType) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setNull(name, sqlType);
		this.statement2.setNull(name, sqlType);
		
		this.replay();
		
		this.statement.setNull(name, sqlType);
		
		this.verify();
	}
	
	/**
	 * @see java.sql.CallableStatement#setNull(java.lang.String, int, java.lang.String)
	 */
	@Test(dataProvider = "string-int-string")
	public void setNull(String name, int sqlType, String typeName) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setNull(name, sqlType, typeName);
		this.statement2.setNull(name, sqlType, typeName);
		
		this.replay();
		
		this.statement.setNull(name, sqlType, typeName);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setObject(name, value);
		this.statement2.setObject(name, value);
		
		this.replay();
		
		this.statement.setObject(name, value);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setObject(name, value, targetSqlType);
		this.statement2.setObject(name, value, targetSqlType);
		
		this.replay();
		
		this.statement.setObject(name, value, targetSqlType);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setObject(name, value, targetSqlType, scale);
		this.statement2.setObject(name, value, targetSqlType, scale);
		
		this.replay();
		
		this.statement.setObject(name, value, targetSqlType, scale);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setShort(name, value);
		this.statement2.setShort(name, value);
		
		this.replay();
		
		this.statement.setShort(name, value);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setString(name, value);
		this.statement2.setString(name, value);
		
		this.replay();
		
		this.statement.setString(name, value);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setTime(name, value);
		this.statement2.setTime(name, value);
		
		this.replay();
		
		this.statement.setTime(name, value);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setTime(name, value, calendar);
		this.statement2.setTime(name, value, calendar);
		
		this.replay();
		
		this.statement.setTime(name, value, calendar);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setTimestamp(name, value);
		this.statement2.setTimestamp(name, value);
		
		this.replay();
		
		this.statement.setTimestamp(name, value);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setTimestamp(name, value, calendar);
		this.statement2.setTimestamp(name, value, calendar);
		
		this.replay();
		
		this.statement.setTimestamp(name, value, calendar);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setURL(name, value);
		this.statement2.setURL(name, value);
		
		this.replay();
		
		this.statement.setURL(name, value);
		
		this.verify();
	}

	public void testWasNull() throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.wasNull()).andReturn(true);
		
		this.replay();
		
		boolean result = this.wasNull();
		
		this.verify();
		
		assert result;
	}
	
	/**
	 * @see java.sql.CallableStatement#wasNull()
	 */
	public boolean wasNull() throws SQLException
	{
		return this.statement.wasNull();
	}

	@Test(dataProvider = "int")
	public void testGetCharacterStream(int index) throws SQLException
	{
		Reader reader = new StringReader("");
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getCharacterStream(index)).andReturn(reader);
		
		this.replay();
		
		Reader result = this.getCharacterStream(index);
		
		this.verify();
		
		assert result == reader;
	}
	
	/**
	 * @see java.sql.CallableStatement#getCharacterStream(int)
	 */
	@Override
	public Reader getCharacterStream(int index) throws SQLException
	{
		return this.statement.getCharacterStream(index);
	}

	@Test(dataProvider = "string")
	public void testGetCharacterStream(String name) throws SQLException
	{
		Reader reader = new StringReader("");
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getCharacterStream(name)).andReturn(reader);
		
		this.replay();
		
		Reader result = this.getCharacterStream(name);
		
		this.verify();
		
		assert result == reader;
	}
	
	/**
	 * @see java.sql.CallableStatement#getCharacterStream(java.lang.String)
	 */
	@Override
	public Reader getCharacterStream(String name) throws SQLException
	{
		return this.statement.getCharacterStream(name);
	}

	@Test(dataProvider = "int")
	public void testGetNCharacterStream(int index) throws SQLException
	{
		Reader reader = new StringReader("");
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getNCharacterStream(index)).andReturn(reader);
		
		this.replay();
		
		Reader result = this.getNCharacterStream(index);
		
		this.verify();
		
		assert result == reader;
	}
	
	/**
	 * @see java.sql.CallableStatement#getNCharacterStream(int)
	 */
	@Override
	public Reader getNCharacterStream(int index) throws SQLException
	{
		return this.statement.getNCharacterStream(index);
	}

	@Test(dataProvider = "string")
	public void testGetNCharacterStream(String name) throws SQLException
	{
		Reader reader = new StringReader("");
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getNCharacterStream(name)).andReturn(reader);
		
		this.replay();
		
		Reader result = this.getNCharacterStream(name);
		
		this.verify();
		
		assert result == reader;
	}
	
	/**
	 * @see java.sql.CallableStatement#getNCharacterStream(java.lang.String)
	 */
	@Override
	public Reader getNCharacterStream(String name) throws SQLException
	{
		return this.statement.getNCharacterStream(name);
	}

	@Test(dataProvider = "int")
	public void testGetNClob(int index) throws SQLException
	{
		NClob clob = EasyMock.createMock(NClob.class);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getNClob(index)).andReturn(clob);
		
		this.replay();
		
		NClob result = this.getNClob(index);
		
		this.verify();
		
		assert result == clob;
	}
	
	/**
	 * @see java.sql.CallableStatement#getNClob(int)
	 */
	@Override
	public NClob getNClob(int index) throws SQLException
	{
		return this.statement.getNClob(index);
	}

	@Test(dataProvider = "string")
	public void testGetNClob(String name) throws SQLException
	{
		NClob clob = EasyMock.createMock(NClob.class);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getNClob(name)).andReturn(clob);
		
		this.replay();
		
		NClob result = this.getNClob(name);
		
		this.verify();
		
		assert result == clob;
	}
	
	/**
	 * @see java.sql.CallableStatement#getNClob(java.lang.String)
	 */
	@Override
	public NClob getNClob(String name) throws SQLException
	{
		return this.statement.getNClob(name);
	}

	@Test(dataProvider = "int")
	public void testGetNString(int index) throws SQLException
	{
		String string = "";
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getNString(index)).andReturn(string);
		
		this.replay();
		
		String result = this.getNString(index);
		
		this.verify();
		
		assert result == string;
	}
	
	/**
	 * @see java.sql.CallableStatement#getNString(int)
	 */
	@Override
	public String getNString(int index) throws SQLException
	{
		return this.statement.getNString(index);
	}

	@Test(dataProvider = "string")
	public void testGetNString(String name) throws SQLException
	{
		String string = "";
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getNString(name)).andReturn(string);
		
		this.replay();
		
		String result = this.getNString(name);
		
		this.verify();
		
		assert result == string;
	}
	
	/**
	 * @see java.sql.CallableStatement#getNString(java.lang.String)
	 */
	@Override
	public String getNString(String name) throws SQLException
	{
		return this.statement.getNString(name);
	}

	@Test(dataProvider = "int")
	public void testGetRowId(int index) throws SQLException
	{
		RowId rowId = EasyMock.createMock(RowId.class);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getRowId(index)).andReturn(rowId);
		
		this.replay();
		
		RowId result = this.getRowId(index);
		
		this.verify();
		
		assert result == rowId;
	}
	
	/**
	 * @see java.sql.CallableStatement#getRowId(int)
	 */
	@Override
	public RowId getRowId(int index) throws SQLException
	{
		return this.statement.getRowId(index);
	}

	@Test(dataProvider = "string")
	public void testGetRowId(String name) throws SQLException
	{
		RowId rowId = EasyMock.createMock(RowId.class);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getRowId(name)).andReturn(rowId);
		
		this.replay();
		
		RowId result = this.getRowId(name);
		
		this.verify();
		
		assert result == rowId;
	}
	
	/**
	 * @see java.sql.CallableStatement#getRowId(java.lang.String)
	 */
	@Override
	public RowId getRowId(String name) throws SQLException
	{
		return this.statement.getRowId(name);
	}

	@Test(dataProvider = "int")
	public void testGetSQLXML(int index) throws SQLException
	{
		SQLXML xml = EasyMock.createMock(SQLXML.class);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getSQLXML(index)).andReturn(xml);
		
		this.replay();
		
		SQLXML result = this.getSQLXML(index);
		
		this.verify();
		
		assert result == xml;
	}
	
	/**
	 * @see java.sql.CallableStatement#getSQLXML(int)
	 */
	@Override
	public SQLXML getSQLXML(int index) throws SQLException
	{
		return this.statement.getSQLXML(index);
	}

	@Test(dataProvider = "string")
	public void testGetSQLXML(String name) throws SQLException
	{
		SQLXML xml = EasyMock.createMock(SQLXML.class);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getSQLXML(name)).andReturn(xml);
		
		this.replay();
		
		SQLXML result = this.getSQLXML(name);
		
		this.verify();
		
		assert result == xml;
	}
	
	/**
	 * @see java.sql.CallableStatement#getSQLXML(java.lang.String)
	 */
	@Override
	public SQLXML getSQLXML(String name) throws SQLException
	{
		return this.statement.getSQLXML(name);
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
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.statement1.setAsciiStream(name, input1);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.statement2.setAsciiStream(name, input2);
		
		this.replay();

		this.statement.setAsciiStream(name, value);
		
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
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.statement1.setAsciiStream(name, input1, length);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.statement2.setAsciiStream(name, input2, length);
		
		this.replay();

		this.statement.setAsciiStream(name, value, length);
		
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
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.statement1.setBinaryStream(name, input1);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.statement2.setBinaryStream(name, input2);
		
		this.replay();

		this.statement.setBinaryStream(name, value);
		
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
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.statement1.setBinaryStream(name, input1, length);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.statement2.setBinaryStream(name, input2, length);
		
		this.replay();

		this.statement.setBinaryStream(name, value, length);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		if (Proxy.isProxyClass(value.getClass()))
		{
			this.statement1.setBlob(name, this.blob1);
			this.statement2.setBlob(name, this.blob2);
		}
		else
		{
			this.statement1.setBlob(EasyMock.eq(name), EasyMock.isA(SerialBlob.class));
			this.statement2.setBlob(EasyMock.eq(name), EasyMock.isA(SerialBlob.class));
		}
		
		this.replay();

		this.statement.setBlob(name, value);
		
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
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.statement1.setBlob(name, input1);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.statement2.setBlob(name, input2);
		
		this.replay();

		this.statement.setBlob(name, value);
		
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
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.statement1.setBlob(name, input1, length);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.statement2.setBlob(name, input2, length);
		
		this.replay();

		this.statement.setBlob(name, value, length);
		
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
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.statement1.setCharacterStream(name, reader1);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.statement2.setCharacterStream(name, reader2);
		
		this.replay();

		this.statement.setCharacterStream(name, value);
		
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
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.statement1.setCharacterStream(name, reader1, length);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.statement2.setCharacterStream(name, reader2, length);
		
		this.replay();

		this.statement.setCharacterStream(name, value, length);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		if (Proxy.isProxyClass(value.getClass()))
		{
			this.statement1.setClob(name, this.clob1);
			this.statement2.setClob(name, this.clob2);
		}
		else
		{
			this.statement1.setClob(EasyMock.eq(name), EasyMock.isA(SerialClob.class));
			this.statement2.setClob(EasyMock.eq(name), EasyMock.isA(SerialClob.class));
		}
		
		this.replay();

		this.statement.setClob(name, value);
		
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
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.statement1.setClob(name, reader1);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.statement2.setClob(name, reader2);
		
		this.replay();

		this.statement.setClob(name, value);
		
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
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.statement1.setClob(name, reader1, length);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.statement2.setClob(name, reader2, length);
		
		this.replay();

		this.statement.setClob(name, value, length);
		
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
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.statement1.setNCharacterStream(name, reader1);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.statement2.setNCharacterStream(name, reader2);
		
		this.replay();

		this.statement.setNCharacterStream(name, value);
		
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
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.statement1.setNCharacterStream(name, reader1, length);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.statement2.setNCharacterStream(name, reader2, length);
		
		this.replay();

		this.statement.setNCharacterStream(name, value, length);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		if (Proxy.isProxyClass(value.getClass()))
		{
			this.statement1.setNClob(name, this.nClob1);
			this.statement2.setNClob(name, this.nClob2);
		}
		else
		{
			this.statement1.setNClob(EasyMock.eq(name), EasyMock.isA(NClob.class));
			this.statement2.setNClob(EasyMock.eq(name), EasyMock.isA(NClob.class));
		}
		
		this.replay();

		this.statement.setNClob(name, value);
		
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
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.statement1.setNClob(name, reader1);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.statement2.setNClob(name, reader2);
		
		this.replay();

		this.statement.setNClob(name, value);
		
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
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.statement1.setNClob(name, reader1, length);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.statement2.setNClob(name, reader2, length);
		
		this.replay();

		this.statement.setNClob(name, value, length);
		
		this.verify();
	}

	/**
	 * @see java.sql.CallableStatement#setNString(java.lang.String, java.lang.String)
	 */
	@Test(dataProvider = "string-string")
	public void setNString(String name, String value) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setNString(name, value);
		this.statement2.setNString(name, value);
		
		this.replay();

		this.statement.setNString(name, value);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setRowId(name, value);
		this.statement2.setRowId(name, value);
		
		this.replay();

		this.statement.setRowId(name, value);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setSQLXML(name, value);
		this.statement2.setSQLXML(name, value);
		
		this.replay();

		this.statement.setSQLXML(name, value);
		
		this.verify();
	}
}
