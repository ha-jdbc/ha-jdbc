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

import java.io.ByteArrayInputStream;
import java.io.CharArrayReader;
import java.io.File;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Collections;
import java.util.Map;

import org.easymock.EasyMock;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.Operation;

/**
 * Unit test for {@link CallableStatement}.
 * 
 * @author  Paul Ferraro
 * @since   1.1
 */
public class TestCallableStatement extends TestPreparedStatement implements java.sql.CallableStatement
{
	/**
	 * @see net.sf.hajdbc.sql.TestingStatement#createStatement(net.sf.hajdbc.sql.Connection)
	 */
	@Override
	protected Statement createStatement(Connection connection) throws SQLException
	{
		Operation<java.sql.Connection, java.sql.CallableStatement> operation = new Operation<java.sql.Connection, java.sql.CallableStatement>()
		{
			public java.sql.CallableStatement execute(Database database, java.sql.Connection connection)
			{
				return TestCallableStatement.this.getSQLStatement();
			}
		};
		
		return new CallableStatement(connection, operation, "sql");
	}
	
	/**
	 * @see net.sf.hajdbc.sql.TestPreparedStatement#getStatementClass()
	 */
	@Override
	protected Class< ? extends java.sql.Statement> getStatementClass()
	{
		return java.sql.CallableStatement.class;
	}

	private CallableStatement getStatement()
	{
		return CallableStatement.class.cast(this.statement);
	}

	@Override
	protected java.sql.CallableStatement getSQLStatement()
	{
		return java.sql.CallableStatement.class.cast(this.sqlStatement);
	}
	
	/**
	 * @see java.sql.CallableStatement#getArray(int)
	 */
	@Test(dataProvider = "int")
	public Array getArray(int index) throws SQLException
	{
		Array array = EasyMock.createMock(Array.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getArray(index)).andReturn(array);
		
		this.control.replay();
		
		Array value = this.getStatement().getArray(index);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getArray(name)).andReturn(array);
		
		this.control.replay();
		
		Array value = this.getStatement().getArray(name);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getBigDecimal(index)).andReturn(decimal);
		
		this.control.replay();
		
		BigDecimal value = this.getStatement().getBigDecimal(index);
		
		this.control.verify();
		
		assert value == decimal;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getBigDecimal(int, int)
	 */
	@Test(dataProvider = "int-int")
	@Deprecated
	public BigDecimal getBigDecimal(int index, int scale) throws SQLException
	{
		BigDecimal decimal = new BigDecimal(10);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getBigDecimal(index, scale)).andReturn(decimal);
		
		this.control.replay();
		
		BigDecimal value = this.getStatement().getBigDecimal(index, scale);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getBigDecimal(name)).andReturn(decimal);
		
		this.control.replay();
		
		BigDecimal value = this.getStatement().getBigDecimal(name);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getBlob(index)).andReturn(blob);
		
		this.control.replay();
		
		Blob value = this.getStatement().getBlob(index);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getBlob(name)).andReturn(blob);
		
		this.control.replay();
		
		Blob value = this.getStatement().getBlob(name);
		
		this.control.verify();
		
		assert value == blob;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getBoolean(int)
	 */
	@Test(dataProvider = "int")
	public boolean getBoolean(int index) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getBoolean(index)).andReturn(true);
		
		this.control.replay();
		
		boolean value = this.getStatement().getBoolean(index);
		
		this.control.verify();
		
		assert value;
		
		return value;
	}

	/**
	 * @see java.sql.CallableStatement#getBoolean(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public boolean getBoolean(String name) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getBoolean(name)).andReturn(true);
		
		this.control.replay();
		
		boolean value = this.getStatement().getBoolean(name);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getByte(index)).andReturn(b);
		
		this.control.replay();
		
		byte value = this.getStatement().getByte(index);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getByte(name)).andReturn(b);
		
		this.control.replay();
		
		byte value = this.getStatement().getByte(name);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getBytes(index)).andReturn(bytes);
		
		this.control.replay();
		
		byte[] value = this.getStatement().getBytes(index);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getBytes(name)).andReturn(bytes);
		
		this.control.replay();
		
		byte[] value = this.getStatement().getBytes(name);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getClob(index)).andReturn(clob);
		
		this.control.replay();
		
		Clob value = this.getStatement().getClob(index);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getClob(name)).andReturn(clob);
		
		this.control.replay();
		
		Clob value = this.getStatement().getClob(name);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getDate(index)).andReturn(date);
		
		this.control.replay();
		
		Date value = this.getStatement().getDate(index);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getDate(index, calendar)).andReturn(date);
		
		this.control.replay();
		
		Date value = this.getStatement().getDate(index, calendar);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getDate(name)).andReturn(date);
		
		this.control.replay();
		
		Date value = this.getStatement().getDate(name);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getDate(name, calendar)).andReturn(date);
		
		this.control.replay();
		
		Date value = this.getStatement().getDate(name, calendar);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getDouble(index)).andReturn(d);
		
		this.control.replay();
		
		double value = this.getStatement().getDouble(index);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getDouble(name)).andReturn(d);
		
		this.control.replay();
		
		double value = this.getStatement().getDouble(name);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getFloat(index)).andReturn(f);
		
		this.control.replay();
		
		float value = this.getStatement().getFloat(index);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getFloat(name)).andReturn(f);
		
		this.control.replay();
		
		float value = this.getStatement().getFloat(name);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getInt(index)).andReturn(i);
		
		this.control.replay();
		
		int value = this.getStatement().getInt(index);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getInt(name)).andReturn(i);
		
		this.control.replay();
		
		int value = this.getStatement().getInt(name);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getLong(index)).andReturn(i);
		
		this.control.replay();
		
		long value = this.getStatement().getLong(index);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getLong(name)).andReturn(i);
		
		this.control.replay();
		
		long value = this.getStatement().getLong(name);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getObject(index)).andReturn(object);
		
		this.control.replay();
		
		Object value = this.getStatement().getObject(index);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getObject(index, map)).andReturn(object);
		
		this.control.replay();
		
		Object value = this.getStatement().getObject(index, map);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getObject(name)).andReturn(object);
		
		this.control.replay();
		
		Object value = this.getStatement().getObject(name);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getObject(name, map)).andReturn(object);
		
		this.control.replay();
		
		Object value = this.getStatement().getObject(name, map);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getRef(index)).andReturn(ref);
		
		this.control.replay();
		
		Ref value = this.getStatement().getRef(index);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getRef(name)).andReturn(ref);
		
		this.control.replay();
		
		Ref value = this.getStatement().getRef(name);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getShort(index)).andReturn(s);
		
		this.control.replay();
		
		short value = this.getStatement().getShort(index);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getShort(name)).andReturn(s);
		
		this.control.replay();
		
		short value = this.getStatement().getShort(name);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getString(index)).andReturn(s);
		
		this.control.replay();
		
		String value = this.getStatement().getString(index);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getString(name)).andReturn(s);
		
		this.control.replay();
		
		String value = this.getStatement().getString(name);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getTime(index)).andReturn(time);
		
		this.control.replay();
		
		Time value = this.getStatement().getTime(index);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getTime(index, calendar)).andReturn(time);
		
		this.control.replay();
		
		Time value = this.getStatement().getTime(index, calendar);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getTime(name)).andReturn(time);
		
		this.control.replay();
		
		Time value = this.getStatement().getTime(name);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getTime(name, calendar)).andReturn(time);
		
		this.control.replay();
		
		Time value = this.getStatement().getTime(name, calendar);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getTimestamp(index)).andReturn(timestamp);
		
		this.control.replay();
		
		Timestamp value = this.getStatement().getTimestamp(index);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getTimestamp(index)).andReturn(timestamp);
		
		this.control.replay();
		
		Timestamp value = this.getStatement().getTimestamp(index);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getTimestamp(name)).andReturn(timestamp);
		
		this.control.replay();
		
		Timestamp value = this.getStatement().getTimestamp(name);
		
		this.control.verify();
		
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
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.getSQLStatement().getTimestamp(name, calendar)).andReturn(timestamp);
		
		this.control.replay();
		
		Timestamp value = this.getStatement().getTimestamp(name, calendar);
		
		this.control.verify();
		
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
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.first()).andReturn(this.database);
			
			EasyMock.expect(this.getSQLStatement().getURL(index)).andReturn(url);
			
			this.control.replay();
			
			URL value = this.getStatement().getURL(index);
			
			this.control.verify();
			
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
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.first()).andReturn(this.database);
			
			EasyMock.expect(this.getSQLStatement().getURL(name)).andReturn(url);
			
			this.control.replay();
			
			URL value = this.getStatement().getURL(name);
			
			this.control.verify();
			
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.getSQLStatement().registerOutParameter(index, sqlType);
		
		this.control.replay();
		
		this.getStatement().registerOutParameter(index, sqlType);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.getSQLStatement().registerOutParameter(index, sqlType, scale);
		
		this.control.replay();
		
		this.getStatement().registerOutParameter(index, sqlType, scale);
		
		this.control.verify();
	}
	
	/**
	 * @see java.sql.CallableStatement#registerOutParameter(int, int, java.lang.String)
	 */
	@Test(dataProvider = "int-int-string")
	public void registerOutParameter(int index, int sqlType, String typeName) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.getSQLStatement().registerOutParameter(index, sqlType, typeName);
		
		this.control.replay();
		
		this.getStatement().registerOutParameter(index, sqlType, typeName);
		
		this.control.verify();
	}
	
	/**
	 * @see java.sql.CallableStatement#registerOutParameter(java.lang.String, int)
	 */
	@Test(dataProvider = "string-int")
	public void registerOutParameter(String name, int sqlType) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.getSQLStatement().registerOutParameter(name, sqlType);
		
		this.control.replay();
		
		this.getStatement().registerOutParameter(name, sqlType);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.getSQLStatement().registerOutParameter(name, sqlType, scale);
		
		this.control.replay();
		
		this.getStatement().registerOutParameter(name, sqlType, scale);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.getSQLStatement().registerOutParameter(name, sqlType, typeName);
		
		this.control.replay();
		
		this.getStatement().registerOutParameter(name, sqlType, typeName);
		
		this.control.verify();
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
		InputStream inputStream = new ByteArrayInputStream(new byte[0]);
		File file = new File("");
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(inputStream);
		
		this.getSQLStatement().setAsciiStream(name, inputStream, 100);
		
		this.control.replay();
		
		this.getStatement().setAsciiStream(name, value, 100);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.getSQLStatement().setBigDecimal(name, value);
		
		this.control.replay();
		
		this.getStatement().setBigDecimal(name, value);
		
		this.control.verify();
	}

	/**
	 * @see java.sql.CallableStatement#setBinaryStream(java.lang.String, java.io.InputStream, int)
	 */
	@Test(dataProvider = "string-inputStream-int")
	public void setBinaryStream(String name, InputStream value, int length) throws SQLException
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[0]);
		File file = new File("");
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(inputStream);
		
		this.getSQLStatement().setBinaryStream(name, inputStream, 100);
		
		this.control.replay();
		
		this.getStatement().setBinaryStream(name, value, 100);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.getSQLStatement().setBoolean(name, value);
		
		this.control.replay();
		
		this.getStatement().setBoolean(name, value);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.getSQLStatement().setByte(name, value);
		
		this.control.replay();
		
		this.getStatement().setByte(name, value);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.getSQLStatement().setBytes(name, value);
		
		this.control.replay();
		
		this.getStatement().setBytes(name, value);
		
		this.control.verify();
	}

	@DataProvider(name = "string-reader-int")
	Object[][] stringReaderProvider()
	{
		return new Object[][] { new Object[] { "column", new CharArrayReader(new char[0]), 0 } };
	}

	/**
	 * @see java.sql.CallableStatement#setCharacterStream(java.lang.String, java.io.Reader, int)
	 */
	@Test(dataProvider = "string-reader-int")
	public void setCharacterStream(String name, Reader value, int length) throws SQLException
	{
		Reader reader = new CharArrayReader(new char[0]);
		File file = new File("");
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader);
		
		this.getSQLStatement().setCharacterStream(name, reader, 100);
		
		this.control.replay();
		
		this.getStatement().setCharacterStream(name, value, 100);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.getSQLStatement().setDate(name, value);
		
		this.control.replay();
		
		this.getStatement().setDate(name, value);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.getSQLStatement().setDate(name, value, calendar);
		
		this.control.replay();
		
		this.getStatement().setDate(name, value, calendar);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.getSQLStatement().setDouble(name, value);
		
		this.control.replay();
		
		this.getStatement().setDouble(name, value);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.getSQLStatement().setFloat(name, value);
		
		this.control.replay();
		
		this.getStatement().setFloat(name, value);
		
		this.control.verify();
	}

	/**
	 * @see java.sql.CallableStatement#setInt(java.lang.String, int)
	 */
	@Test(dataProvider = "string-int")
	public void setInt(String name, int value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.getSQLStatement().setInt(name, value);
		
		this.control.replay();
		
		this.getStatement().setInt(name, value);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.getSQLStatement().setLong(name, value);
		
		this.control.replay();
		
		this.getStatement().setLong(name, value);
		
		this.control.verify();
	}

	/**
	 * @see java.sql.CallableStatement#setNull(java.lang.String, int)
	 */
	@Test(dataProvider = "string-int")
	public void setNull(String name, int sqlType) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.getSQLStatement().setNull(name, sqlType);
		
		this.control.replay();
		
		this.getStatement().setNull(name, sqlType);
		
		this.control.verify();
	}
	
	/**
	 * @see java.sql.CallableStatement#setNull(java.lang.String, int, java.lang.String)
	 */
	@Test(dataProvider = "string-int-string")
	public void setNull(String name, int sqlType, String typeName) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.getSQLStatement().setNull(name, sqlType, typeName);
		
		this.control.replay();
		
		this.getStatement().setNull(name, sqlType, typeName);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.getSQLStatement().setObject(name, value);
		
		this.control.replay();
		
		this.getStatement().setObject(name, value);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.getSQLStatement().setObject(name, value, targetSqlType);
		
		this.control.replay();
		
		this.getStatement().setObject(name, value, targetSqlType);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.getSQLStatement().setObject(name, value, targetSqlType, scale);
		
		this.control.replay();
		
		this.getStatement().setObject(name, value, targetSqlType, scale);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.getSQLStatement().setShort(name, value);
		
		this.control.replay();
		
		this.getStatement().setShort(name, value);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.getSQLStatement().setString(name, value);
		
		this.control.replay();
		
		this.getStatement().setString(name, value);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.getSQLStatement().setTime(name, value);
		
		this.control.replay();
		
		this.getStatement().setTime(name, value);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.getSQLStatement().setTime(name, value, calendar);
		
		this.control.replay();
		
		this.getStatement().setTime(name, value, calendar);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.getSQLStatement().setTimestamp(name, value);
		
		this.control.replay();
		
		this.getStatement().setTimestamp(name, value);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.getSQLStatement().setTimestamp(name, value, calendar);
		
		this.control.replay();
		
		this.getStatement().setTimestamp(name, value, calendar);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.getSQLStatement().setURL(name, value);
		
		this.control.replay();
		
		this.getStatement().setURL(name, value);
		
		this.control.verify();
	}

	/**
	 * @see java.sql.CallableStatement#wasNull()
	 */
	@Test
	public boolean wasNull() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		EasyMock.expect(this.getSQLStatement().wasNull()).andReturn(true);
		
		this.control.replay();
		
		boolean result = this.getStatement().wasNull();
		
		this.control.verify();
		
		assert result;
		
		return false;
	}
}
