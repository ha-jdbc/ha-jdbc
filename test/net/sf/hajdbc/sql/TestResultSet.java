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
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseMetaDataCache;
import net.sf.hajdbc.DatabaseProperties;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.LockManager;
import net.sf.hajdbc.MockDatabase;
import net.sf.hajdbc.TableProperties;
import net.sf.hajdbc.util.reflect.ProxyFactory;

import org.easymock.EasyMock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/***
 * @author  Paul Ferraro
 * @since   1.0
 */
@SuppressWarnings("unchecked")
public class TestResultSet implements ResultSet
{
	private Balancer balancer = EasyMock.createStrictMock(Balancer.class);
	private DatabaseCluster cluster = EasyMock.createStrictMock(DatabaseCluster.class);
	private FileSupport fileSupport = EasyMock.createStrictMock(FileSupport.class);
	private Lock readLock = EasyMock.createStrictMock(Lock.class);
	private Lock writeLock1 = EasyMock.createStrictMock(Lock.class);
	private Lock writeLock2 = EasyMock.createStrictMock(Lock.class);
	private LockManager lockManager = EasyMock.createStrictMock(LockManager.class);
	private Dialect dialect = EasyMock.createStrictMock(Dialect.class);
	private DatabaseMetaDataCache metaData = EasyMock.createStrictMock(DatabaseMetaDataCache.class);
	private DatabaseProperties databaseProperties = EasyMock.createStrictMock(DatabaseProperties.class);
	private TableProperties tableProperties = EasyMock.createStrictMock(TableProperties.class);
	private ColumnProperties columnProperties = EasyMock.createStrictMock(ColumnProperties.class);
	private ResultSet resultSet1 = EasyMock.createStrictMock(ResultSet.class);
	private ResultSet resultSet2 = EasyMock.createStrictMock(ResultSet.class);
	private SQLProxy parent = EasyMock.createStrictMock(SQLProxy.class);
	private SQLProxy root = EasyMock.createStrictMock(SQLProxy.class);
	private Blob blob1 = EasyMock.createMock(Blob.class);
	private Blob blob2 = EasyMock.createMock(Blob.class);
	private Clob clob1 = EasyMock.createMock(Clob.class);
	private Clob clob2 = EasyMock.createMock(Clob.class);
	private NClob nClob1 = EasyMock.createMock(NClob.class);
	private NClob nClob2 = EasyMock.createMock(NClob.class);
	
	private Database database1 = new MockDatabase("1");
	private Database database2 = new MockDatabase("2");
	private Set<Database> databaseSet;
	private ExecutorService executor = Executors.newSingleThreadExecutor();
	private Statement statement = EasyMock.createMock(Statement.class);
	private ResultSet resultSet;
	private ResultSetInvocationHandler handler;
	
	@BeforeClass
	void init() throws Exception
	{
		Map<Database, ResultSet> map = new TreeMap<Database, ResultSet>();
		map.put(this.database1, this.resultSet1);
		map.put(this.database2, this.resultSet2);
		
		this.databaseSet = map.keySet();
		
		EasyMock.expect(this.parent.getDatabaseCluster()).andReturn(this.cluster);
		
		this.parent.addChild(EasyMock.isA(ResultSetInvocationHandler.class));
		
		this.replay();
		
		this.handler = new ResultSetInvocationHandler(this.statement, this.parent, EasyMock.createMock(Invoker.class), map, this.fileSupport);
		this.resultSet = ProxyFactory.createProxy(ResultSet.class, this.handler);
		
		this.verify();
		this.reset();
	}
	
	private Object[] objects()
	{
		return new Object[] { this.cluster, this.balancer, this.resultSet1, this.resultSet2, this.fileSupport, this.readLock, this.writeLock1, this.writeLock2, this.lockManager, this.parent, this.root, this.dialect, this.metaData, this.databaseProperties, this.tableProperties, this.columnProperties };
	}
	
	void replay()
	{
		EasyMock.replay(this.objects());
	}
	
	void verify()
	{
		EasyMock.verify(this.objects());
	}
	
	@AfterMethod
	void reset()
	{
		EasyMock.reset(this.objects());
	}

	@DataProvider(name = "int")
	Object[][] intProvider()
	{
		return new Object[][] { new Object[] { 1 } };
	}
	
	/**
	 * @see java.sql.ResultSet#absolute(int)
	 */
	@Test(dataProvider = "int")
	public boolean absolute(int row) throws SQLException
	{
		EasyMock.expect(this.resultSet1.absolute(row)).andReturn(true);
		EasyMock.expect(this.resultSet2.absolute(row)).andReturn(true);
		
		this.replay();
		
		boolean valid = this.resultSet.absolute(row);
		
		this.verify();
		
		assert valid;
		
		return valid;
	}

	/**
	 * @see java.sql.ResultSet#afterLast()
	 */
	@Test
	public void afterLast() throws SQLException
	{
		this.resultSet1.afterLast();
		this.resultSet2.afterLast();
		
		this.replay();
		
		this.resultSet.afterLast();
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#beforeFirst()
	 */
	@Test
	public void beforeFirst() throws SQLException
	{
		this.resultSet1.beforeFirst();
		this.resultSet2.beforeFirst();
		
		this.replay();
		
		this.resultSet.beforeFirst();
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#cancelRowUpdates()
	 */
	@Test
	public void cancelRowUpdates() throws SQLException
	{
		this.resultSet1.cancelRowUpdates();
		this.resultSet2.cancelRowUpdates();
		
		this.replay();
		
		this.resultSet.cancelRowUpdates();
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#clearWarnings()
	 */
	@Test
	public void clearWarnings() throws SQLException
	{
		this.resultSet1.clearWarnings();
		this.resultSet2.clearWarnings();
		
		this.replay();
		
		this.resultSet.clearWarnings();
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#close()
	 */
	@Test
	public void close() throws SQLException
	{
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		this.resultSet1.close();
		this.resultSet2.close();

		this.parent.removeChild(this.handler);
		
		this.replay();
		
		this.resultSet.close();
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#deleteRow()
	 */
	@Test
	public void deleteRow() throws SQLException
	{
		EasyMock.expect(this.cluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.readLock);
		
		this.readLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		this.resultSet1.deleteRow();
		this.resultSet2.deleteRow();
		
		this.readLock.unlock();
		
		this.replay();
		
		this.resultSet.deleteRow();
		
		this.verify();
	}

	@DataProvider(name = "string")
	Object[][] stringProvider()
	{
		return new Object[][] { new Object[] { "" } };
	}
	
	/**
	 * @see java.sql.ResultSet#findColumn(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public int findColumn(String name) throws SQLException
	{
		EasyMock.expect(this.resultSet1.findColumn(name)).andReturn(1);
		
		this.replay();
		
		int result = this.resultSet.findColumn(name);
		
		this.verify();
		
		assert result == 1 : result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#first()
	 */
	@Test
	public boolean first() throws SQLException
	{
		EasyMock.expect(this.resultSet1.first()).andReturn(true);
		EasyMock.expect(this.resultSet2.first()).andReturn(true);
		
		this.replay();
		
		boolean result = this.resultSet.first();
		
		this.verify();
		
		assert result;
				
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getArray(int)
	 */
	@Test(dataProvider = "int")
	public Array getArray(int index) throws SQLException
	{
		Array array = EasyMock.createMock(Array.class);
		
		EasyMock.expect(this.resultSet1.getArray(index)).andReturn(array);
		
		this.replay();
		
		Array result = this.resultSet.getArray(index);
		
		this.verify();
		
		assert array == result;
		
		return result;
	}
	
	/**
	 * @see java.sql.ResultSet#getArray(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public Array getArray(String name) throws SQLException
	{
		Array array = EasyMock.createMock(Array.class);
		
		EasyMock.expect(this.resultSet1.getArray(name)).andReturn(array);
		
		this.replay();
		
		Array result = this.resultSet.getArray(name);
		
		this.verify();
		
		assert array == result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getAsciiStream(int)
	 */
	@Test(dataProvider = "int")
	public InputStream getAsciiStream(int index) throws SQLException
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.resultSet1.getAsciiStream(index)).andReturn(inputStream);
		
		this.replay();
		
		InputStream result = this.resultSet.getAsciiStream(index);
		
		this.verify();
		
		assert inputStream == result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getAsciiStream(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public InputStream getAsciiStream(String name) throws SQLException
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.resultSet1.getAsciiStream(name)).andReturn(inputStream);
		
		this.replay();
		
		InputStream result = this.resultSet.getAsciiStream(name);
		
		this.verify();
		
		assert inputStream == result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getBigDecimal(int)
	 */
	@Test(dataProvider = "int")
	public BigDecimal getBigDecimal(int index) throws SQLException
	{
		BigDecimal decimal = new BigDecimal(1.0);
		
		EasyMock.expect(this.resultSet1.getBigDecimal(index)).andReturn(decimal);
		
		this.replay();
		
		BigDecimal result = this.resultSet.getBigDecimal(index);
		
		this.verify();
		
		assert decimal == result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getBigDecimal(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public BigDecimal getBigDecimal(String name) throws SQLException
	{
		BigDecimal decimal = new BigDecimal(1.0);
		
		EasyMock.expect(this.resultSet1.getBigDecimal(name)).andReturn(decimal);
		
		this.replay();
		
		BigDecimal result = this.resultSet.getBigDecimal(name);
		
		this.verify();
		
		assert decimal == result;
		
		return result;
	}

	@DataProvider(name = "int-int")
	Object[][] intIntProvider()
	{
		return new Object[][] { new Object[] { 1, 1 } };
	}
	
	/**
	 * @see java.sql.ResultSet#getBigDecimal(int, int)
	 */
	@SuppressWarnings("deprecation")
	@Test(dataProvider = "int-int")
	@Deprecated
	public BigDecimal getBigDecimal(int index, int scale) throws SQLException
	{
		BigDecimal decimal = new BigDecimal(1.0);
		
		EasyMock.expect(this.resultSet1.getBigDecimal(index, scale)).andReturn(decimal);
		
		this.replay();
		
		BigDecimal result = this.resultSet.getBigDecimal(index, scale);
		
		this.verify();
		
		assert decimal == result;
		
		return result;
	}

	@DataProvider(name = "string-int")
	Object[][] stringIntProvider()
	{
		return new Object[][] { new Object[] { "", 1 } };
	}

	/**
	 * @see java.sql.ResultSet#getBigDecimal(java.lang.String, int)
	 */
	@SuppressWarnings("deprecation")
	@Test(dataProvider = "string-int")
	@Deprecated
	public BigDecimal getBigDecimal(String name, int scale) throws SQLException
	{
		BigDecimal decimal = new BigDecimal(1.0);
		
		EasyMock.expect(this.resultSet1.getBigDecimal(name, scale)).andReturn(decimal);
		
		this.replay();
		
		BigDecimal result = this.resultSet.getBigDecimal(name, scale);
		
		this.verify();
		
		assert decimal == result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getBinaryStream(int)
	 */
	@Test(dataProvider = "int")
	public InputStream getBinaryStream(int index) throws SQLException
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.resultSet1.getBinaryStream(index)).andReturn(inputStream);
		
		this.replay();
		
		InputStream result = this.resultSet.getBinaryStream(index);
		
		this.verify();
		
		assert inputStream == result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getBinaryStream(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public InputStream getBinaryStream(String name) throws SQLException
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.resultSet1.getBinaryStream(name)).andReturn(inputStream);
		
		this.replay();
		
		InputStream result = this.resultSet.getBinaryStream(name);
		
		this.verify();
		
		assert inputStream == result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getBlob(int)
	 */
	@Test(dataProvider = "int")
	public Blob getBlob(int index) throws SQLException
	{
		Blob blob1 = EasyMock.createMock(Blob.class);
		Blob blob2 = EasyMock.createMock(Blob.class);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.resultSet1.getBlob(index)).andReturn(blob1);
		EasyMock.expect(this.resultSet2.getBlob(index)).andReturn(blob2);
		
		this.replay();
		
		Blob result = this.resultSet.getBlob(index);
		
		this.verify();

		assert Proxy.isProxyClass(result.getClass());
		
		BlobInvocationHandler handler = BlobInvocationHandler.class.cast(Proxy.getInvocationHandler(result));
		
		assert handler.getObject(this.database1) == blob1;
		assert handler.getObject(this.database2) == blob2;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getBlob(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public Blob getBlob(String name) throws SQLException
	{
		Blob blob1 = EasyMock.createMock(Blob.class);
		Blob blob2 = EasyMock.createMock(Blob.class);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.resultSet1.getBlob(name)).andReturn(blob1);
		EasyMock.expect(this.resultSet2.getBlob(name)).andReturn(blob2);
		
		this.replay();
		
		Blob result = this.resultSet.getBlob(name);
		
		this.verify();

		assert Proxy.isProxyClass(result.getClass());
		
		BlobInvocationHandler handler = BlobInvocationHandler.class.cast(Proxy.getInvocationHandler(result));
		
		assert handler.getObject(this.database1) == blob1;
		assert handler.getObject(this.database2) == blob2;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getBoolean(int)
	 */
	@Test(dataProvider = "int")
	public boolean getBoolean(int index) throws SQLException
	{
		EasyMock.expect(this.resultSet1.getBoolean(index)).andReturn(true);
		
		this.replay();
		
		boolean result = this.resultSet.getBoolean(index);
		
		this.verify();
		
		assert result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getBoolean(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public boolean getBoolean(String name) throws SQLException
	{
		EasyMock.expect(this.resultSet1.getBoolean(name)).andReturn(true);
		
		this.replay();
		
		boolean result = this.resultSet.getBoolean(name);
		
		this.verify();
		
		assert result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getByte(int)
	 */
	@Test(dataProvider = "int")
	public byte getByte(int index) throws SQLException
	{
		byte b = Integer.valueOf(1).byteValue();
		
		EasyMock.expect(this.resultSet1.getByte(index)).andReturn(b);
		
		this.replay();
		
		byte result = this.resultSet.getByte(index);
		
		this.verify();
		
		assert b == result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getByte(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public byte getByte(String name) throws SQLException
	{
		byte b = Integer.valueOf(1).byteValue();
		
		EasyMock.expect(this.resultSet1.getByte(name)).andReturn(b);
		
		this.replay();
		
		byte result = this.resultSet.getByte(name);
		
		this.verify();
		
		assert b == result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getBytes(int)
	 */
	@Test(dataProvider = "int")
	public byte[] getBytes(int index) throws SQLException
	{
		byte[] bytes = new byte[0];
		
		EasyMock.expect(this.resultSet1.getBytes(index)).andReturn(bytes);
		
		this.replay();
		
		byte[] result = this.resultSet.getBytes(index);
		
		this.verify();
		
		assert bytes == result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getBytes(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public byte[] getBytes(String name) throws SQLException
	{
		byte[] bytes = new byte[0];
		
		EasyMock.expect(this.resultSet1.getBytes(name)).andReturn(bytes);
		
		this.replay();
		
		byte[] result = this.resultSet.getBytes(name);
		
		this.verify();
		
		assert bytes == result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getCharacterStream(int)
	 */
	@Test(dataProvider = "int")
	public Reader getCharacterStream(int index) throws SQLException
	{
		Reader reader = new CharArrayReader(new char[0]);
		
		EasyMock.expect(this.resultSet1.getCharacterStream(index)).andReturn(reader);
		
		this.replay();
		
		Reader result = this.resultSet.getCharacterStream(index);
		
		this.verify();
		
		assert result == reader;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getCharacterStream(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public Reader getCharacterStream(String name) throws SQLException
	{
		Reader reader = new CharArrayReader(new char[0]);
		
		EasyMock.expect(this.resultSet1.getCharacterStream(name)).andReturn(reader);
		
		this.replay();
		
		Reader result = this.resultSet.getCharacterStream(name);
		
		this.verify();
		
		assert result == reader;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getClob(int)
	 */
	@Test(dataProvider = "int")
	public Clob getClob(int index) throws SQLException
	{
		Clob clob1 = EasyMock.createMock(Clob.class);
		Clob clob2 = EasyMock.createMock(Clob.class);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.resultSet1.getClob(index)).andReturn(clob1);
		EasyMock.expect(this.resultSet2.getClob(index)).andReturn(clob2);
		
		this.replay();
		
		Clob result = this.resultSet.getClob(index);
		
		this.verify();

		assert Proxy.isProxyClass(result.getClass());
		
		ClobInvocationHandler handler = ClobInvocationHandler.class.cast(Proxy.getInvocationHandler(result));
		
		assert handler.getObject(this.database1) == clob1;
		assert handler.getObject(this.database2) == clob2;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getClob(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public Clob getClob(String name) throws SQLException
	{
		Clob clob1 = EasyMock.createMock(Clob.class);
		Clob clob2 = EasyMock.createMock(Clob.class);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.resultSet1.getClob(name)).andReturn(clob1);
		EasyMock.expect(this.resultSet2.getClob(name)).andReturn(clob2);
		
		this.replay();
		
		Clob result = this.resultSet.getClob(name);
		
		this.verify();

		assert Proxy.isProxyClass(result.getClass());
		
		ClobInvocationHandler handler = ClobInvocationHandler.class.cast(Proxy.getInvocationHandler(result));
		
		assert handler.getObject(this.database1) == clob1;
		assert handler.getObject(this.database2) == clob2;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getConcurrency()
	 */
	@Test
	public int getConcurrency() throws SQLException
	{
		int concurrency = java.sql.ResultSet.CONCUR_READ_ONLY;
		
		EasyMock.expect(this.resultSet1.getConcurrency()).andReturn(concurrency);
		
		this.replay();
		
		int result = this.resultSet.getConcurrency();
		
		this.verify();
		
		assert result == concurrency;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getCursorName()
	 */
	@Test
	public String getCursorName() throws SQLException
	{
		String cursor = "cursor";
		
		EasyMock.expect(this.resultSet1.getCursorName()).andReturn(cursor);
		
		this.replay();
		
		String result = this.resultSet.getCursorName();
		
		this.verify();
		
		assert result == cursor;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getDate(int)
	 */
	@Test(dataProvider = "int")
	public Date getDate(int index) throws SQLException
	{
		Date date = new Date(System.currentTimeMillis());
		
		EasyMock.expect(this.resultSet1.getDate(index)).andReturn(date);
		
		this.replay();
		
		Date result = this.resultSet.getDate(index);
		
		this.verify();
		
		assert result == date;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getDate(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public Date getDate(String name) throws SQLException
	{
		Date date = new Date(System.currentTimeMillis());
		
		EasyMock.expect(this.resultSet1.getDate(name)).andReturn(date);
		
		this.replay();
		
		Date result = this.resultSet.getDate(name);
		
		this.verify();
		
		assert result == date;
		
		return result;
	}

	@DataProvider(name = "int-calendar")
	Object[][] intCalendarProvider()
	{
		return new Object[][] { new Object[] { 1, Calendar.getInstance() } };
	}
	
	/**
	 * @see java.sql.ResultSet#getDate(int, java.util.Calendar)
	 */
	@Test(dataProvider = "int-calendar")
	public Date getDate(int index, Calendar calendar) throws SQLException
	{
		Date date = new Date(System.currentTimeMillis());
		
		EasyMock.expect(this.resultSet1.getDate(index, calendar)).andReturn(date);
		
		this.replay();
		
		Date result = this.resultSet.getDate(index, calendar);
		
		this.verify();
		
		assert result == date;
		
		return result;
	}

	@DataProvider(name = "string-calendar")
	Object[][] stringCalendarProvider()
	{
		return new Object[][] { new Object[] { "", Calendar.getInstance() } };
	}

	/**
	 * @see java.sql.ResultSet#getDate(java.lang.String, java.util.Calendar)
	 */
	@Test(dataProvider = "string-calendar")
	public Date getDate(String name, Calendar calendar) throws SQLException
	{
		Date date = new Date(System.currentTimeMillis());
		
		EasyMock.expect(this.resultSet1.getDate(name, calendar)).andReturn(date);
		
		this.replay();
		
		Date result = this.resultSet.getDate(name, calendar);
		
		this.verify();
		
		assert result == date;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getDouble(int)
	 */
	@Test(dataProvider = "int")
	public double getDouble(int index) throws SQLException
	{
		double d = 1.0;
		
		EasyMock.expect(this.resultSet1.getDouble(index)).andReturn(d);
		
		this.replay();
		
		double result = this.resultSet.getDouble(index);
		
		this.verify();
		
		assert result == d;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getDouble(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public double getDouble(String name) throws SQLException
	{
		double d = 1.0;
		
		EasyMock.expect(this.resultSet1.getDouble(name)).andReturn(d);
		
		this.replay();
		
		double result = this.resultSet.getDouble(name);
		
		this.verify();
		
		assert result == d;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getFetchDirection()
	 */
	@Test
	public int getFetchDirection() throws SQLException
	{
		int direction = java.sql.ResultSet.FETCH_FORWARD;
		
		EasyMock.expect(this.resultSet1.getFetchDirection()).andReturn(direction);
		
		this.replay();
		
		int result = this.resultSet.getFetchDirection();
		
		this.verify();
		
		assert result == direction;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getFetchSize()
	 */
	@Test
	public int getFetchSize() throws SQLException
	{
		int size = 10;
		
		EasyMock.expect(this.resultSet1.getFetchSize()).andReturn(size);
		
		this.replay();
		
		int result = this.resultSet.getFetchSize();
		
		this.verify();
		
		assert result == size;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getFloat(int)
	 */
	@Test(dataProvider = "int")
	public float getFloat(int index) throws SQLException
	{
		float f = 1.0F;
		
		EasyMock.expect(this.resultSet1.getFloat(index)).andReturn(f);
		
		this.replay();
		
		float result = this.resultSet.getFloat(index);
		
		this.verify();
		
		assert result == f;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getFloat(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public float getFloat(String name) throws SQLException
	{
		float f = 1.0F;
		
		EasyMock.expect(this.resultSet1.getFloat(name)).andReturn(f);
		
		this.replay();
		
		float result = this.resultSet.getFloat(name);
		
		this.verify();
		
		assert result == f;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getInt(int)
	 */
	@Test(dataProvider = "int")
	public int getInt(int index) throws SQLException
	{
		int i = 1;
		
		EasyMock.expect(this.resultSet1.getInt(index)).andReturn(i);
		
		this.replay();
		
		int result = this.resultSet.getInt(index);
		
		this.verify();
		
		assert result == i;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getInt(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public int getInt(String name) throws SQLException
	{
		int i = 1;
		
		EasyMock.expect(this.resultSet1.getInt(name)).andReturn(i);
		
		this.replay();
		
		int result = this.resultSet.getInt(name);
		
		this.verify();
		
		assert result == i;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getLong(int)
	 */
	@Test(dataProvider = "int")
	public long getLong(int index) throws SQLException
	{
		long l = 1;
		
		EasyMock.expect(this.resultSet1.getLong(index)).andReturn(l);
		
		this.replay();
		
		long result = this.resultSet.getLong(index);
		
		this.verify();
		
		assert result == l;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getLong(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public long getLong(String name) throws SQLException
	{
		long l = 1;
		
		EasyMock.expect(this.resultSet1.getLong(name)).andReturn(l);
		
		this.replay();
		
		long result = this.resultSet.getLong(name);
		
		this.verify();
		
		assert result == l;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getMetaData()
	 */
	@Test
	public ResultSetMetaData getMetaData() throws SQLException
	{
		ResultSetMetaData metaData = EasyMock.createMock(ResultSetMetaData.class);
		
		EasyMock.expect(this.resultSet1.getMetaData()).andReturn(metaData);
		
		this.replay();
		
		ResultSetMetaData result = this.resultSet.getMetaData();
		
		this.verify();
		
		assert result == metaData;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getObject(int)
	 */
	@Test(dataProvider = "int")
	public Object getObject(int index) throws SQLException
	{
		Object object = new Object();
				
		EasyMock.expect(this.resultSet1.getObject(index)).andReturn(object);
		
		this.replay();
		
		Object result = this.resultSet.getObject(index);
		
		this.verify();
		
		assert result == object;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getObject(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public Object getObject(String name) throws SQLException
	{
		Object object = new Object();
		
		EasyMock.expect(this.resultSet1.getObject(name)).andReturn(object);
		
		this.replay();
		
		Object result = this.resultSet.getObject(name);
		
		this.verify();
		
		assert result == object;
		
		return result;
	}

	@DataProvider(name = "int-map")
	Object[][] intMapProvider()
	{
		return new Object[][] { new Object[] { 1, Collections.EMPTY_MAP } };
	}
	
	/**
	 * @see java.sql.ResultSet#getObject(int, java.util.Map)
	 */
	@Test(dataProvider = "int-map")
	public Object getObject(int index, Map<String, Class<?>> map) throws SQLException
	{
		Object object = new Object();
		
		EasyMock.expect(this.resultSet1.getObject(index, map)).andReturn(object);
		
		this.replay();
		
		Object result = this.resultSet.getObject(index, map);
		
		this.verify();
		
		assert result == object;
		
		return result;
	}

	@DataProvider(name = "string-map")
	Object[][] stringMapProvider()
	{
		return new Object[][] { new Object[] { "", Collections.EMPTY_MAP } };
	}

	/**
	 * @see java.sql.ResultSet#getObject(java.lang.String, java.util.Map)
	 */
	@Test(dataProvider = "string-map")
	public Object getObject(String name, Map<String, Class<?>> map) throws SQLException
	{
		Object object = new Object();
		
		EasyMock.expect(this.resultSet1.getObject(name, map)).andReturn(object);
		
		this.replay();
		
		Object result = this.resultSet.getObject(name, map);
		
		this.verify();
		
		assert result == object;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getRef(int)
	 */
	@Test(dataProvider = "int")
	public Ref getRef(int index) throws SQLException
	{
		Ref ref = EasyMock.createMock(Ref.class);
		
		EasyMock.expect(this.resultSet1.getRef(index)).andReturn(ref);
		
		this.replay();
		
		Ref result = this.resultSet.getRef(index);
		
		this.verify();
		
		assert result == ref;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getRef(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public Ref getRef(String name) throws SQLException
	{
		Ref ref = EasyMock.createMock(Ref.class);
		
		EasyMock.expect(this.resultSet1.getRef(name)).andReturn(ref);
		
		this.replay();
		
		Ref result = this.resultSet.getRef(name);
		
		this.verify();
		
		assert result == ref;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getRow()
	 */
	@Test
	public int getRow() throws SQLException
	{
		int row = 1;
		
		EasyMock.expect(this.resultSet1.getRow()).andReturn(row);
		
		this.replay();
		
		int result = this.resultSet.getRow();
		
		this.verify();
		
		assert result == row;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getShort(int)
	 */
	@Test(dataProvider = "int")
	public short getShort(int index) throws SQLException
	{
		short s = Integer.valueOf(1).shortValue();
		
		EasyMock.expect(this.resultSet1.getShort(index)).andReturn(s);
		
		this.replay();
		
		short result = this.resultSet.getShort(index);
		
		this.verify();
		
		assert result == s;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getShort(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public short getShort(String name) throws SQLException
	{
		short s = Integer.valueOf(1).shortValue();
		
		EasyMock.expect(this.resultSet1.getShort(name)).andReturn(s);
		
		this.replay();
		
		short result = this.resultSet.getShort(name);
		
		this.verify();
		
		assert result == s;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getStatement()
	 */
	@Test
	public java.sql.Statement getStatement() throws SQLException
	{
		this.replay();
		
		java.sql.Statement result = this.resultSet.getStatement();
		
		this.verify();
		
		assert result == this.statement;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getString(int)
	 */
	@Test(dataProvider = "int")
	public String getString(int index) throws SQLException
	{
		String string = "";
		
		EasyMock.expect(this.resultSet1.getString(index)).andReturn(string);
		
		this.replay();
		
		String result = this.resultSet.getString(index);
		
		this.verify();
		
		assert result == string;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getString(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public String getString(String name) throws SQLException
	{
		String string = "";
		
		EasyMock.expect(this.resultSet1.getString(name)).andReturn(string);
		
		this.replay();
		
		String result = this.resultSet.getString(name);
		
		this.verify();
		
		assert result == string;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getTime(int)
	 */
	@Test(dataProvider = "int")
	public Time getTime(int index) throws SQLException
	{
		Time time = new Time(System.currentTimeMillis());
		
		EasyMock.expect(this.resultSet1.getTime(index)).andReturn(time);
		
		this.replay();
		
		Time result = this.resultSet.getTime(index);
		
		this.verify();
		
		assert result == time;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getTime(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public Time getTime(String name) throws SQLException
	{
		Time time = new Time(System.currentTimeMillis());
		
		EasyMock.expect(this.resultSet1.getTime(name)).andReturn(time);
		
		this.replay();
		
		Time result = this.resultSet.getTime(name);
		
		this.verify();
		
		assert result == time;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getTime(int, java.util.Calendar)
	 */
	@Test(dataProvider = "int-calendar")
	public Time getTime(int index, Calendar calendar) throws SQLException
	{
		Time time = new Time(System.currentTimeMillis());
		
		EasyMock.expect(this.resultSet1.getTime(index, calendar)).andReturn(time);
		
		this.replay();
		
		Time result = this.resultSet.getTime(index, calendar);
		
		this.verify();
		
		assert result == time;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getTime(java.lang.String, java.util.Calendar)
	 */
	@Test(dataProvider = "string-calendar")
	public Time getTime(String name, Calendar calendar) throws SQLException
	{
		Time time = new Time(System.currentTimeMillis());
		
		EasyMock.expect(this.resultSet1.getTime(name, calendar)).andReturn(time);
		
		this.replay();
		
		Time result = this.resultSet.getTime(name, calendar);
		
		this.verify();
		
		assert result == time;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getTimestamp(int)
	 */
	@Test(dataProvider = "int")
	public Timestamp getTimestamp(int index) throws SQLException
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		
		EasyMock.expect(this.resultSet1.getTimestamp(index)).andReturn(timestamp);
		
		this.replay();
		
		Timestamp result = this.resultSet.getTimestamp(index);
		
		this.verify();
		
		assert result == timestamp;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getTimestamp(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public Timestamp getTimestamp(String name) throws SQLException
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		
		EasyMock.expect(this.resultSet1.getTimestamp(name)).andReturn(timestamp);
		
		this.replay();
		
		Timestamp result = this.resultSet.getTimestamp(name);
		
		this.verify();
		
		assert result == timestamp;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getTimestamp(int, java.util.Calendar)
	 */
	@Test(dataProvider = "int-calendar")
	public Timestamp getTimestamp(int index, Calendar calendar) throws SQLException
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		
		EasyMock.expect(this.resultSet1.getTimestamp(index, calendar)).andReturn(timestamp);
		
		this.replay();
		
		Timestamp result = this.resultSet.getTimestamp(index, calendar);
		
		this.verify();
		
		assert result == timestamp;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getTimestamp(java.lang.String, java.util.Calendar)
	 */
	@Test(dataProvider = "string-calendar")
	public Timestamp getTimestamp(String name, Calendar calendar) throws SQLException
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		
		EasyMock.expect(this.resultSet1.getTimestamp(name, calendar)).andReturn(timestamp);
		
		this.replay();
		
		Timestamp result = this.resultSet.getTimestamp(name, calendar);
		
		this.verify();
		
		assert result == timestamp;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getType()
	 */
	@Test
	public int getType() throws SQLException
	{
		int type = java.sql.ResultSet.TYPE_FORWARD_ONLY;
		
		EasyMock.expect(this.resultSet1.getType()).andReturn(type);
		
		this.replay();
		
		int result = this.resultSet.getType();
		
		this.verify();
		
		assert result == type;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getURL(int)
	 */
	@Test(dataProvider = "int")
	public URL getURL(int index) throws SQLException
	{
		try
		{
			URL url = new URL("http://ha-jdbc.sf.net");
			
			EasyMock.expect(this.resultSet1.getURL(index)).andReturn(url);
			
			this.replay();
			
			URL result = this.resultSet.getURL(index);
			
			this.verify();
			
			assert result == url;
			
			return result;
		}
		catch (MalformedURLException e)
		{
			assert false;
			return null;
		}
	}

	/**
	 * @see java.sql.ResultSet#getURL(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public URL getURL(String name) throws SQLException
	{
		try
		{
			URL url = new URL("http://ha-jdbc.sf.net");
			
			EasyMock.expect(this.resultSet1.getURL(name)).andReturn(url);
			
			this.replay();
			
			URL result = this.resultSet.getURL(name);
			
			this.verify();
			
			assert result == url;
			
			return result;
		}
		catch (MalformedURLException e)
		{
			assert false;
			return null;
		}
	}

	/**
	 * @see java.sql.ResultSet#getUnicodeStream(int)
	 */
	@SuppressWarnings("deprecation")
	@Test(dataProvider = "int")
	@Deprecated
	public InputStream getUnicodeStream(int index) throws SQLException
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.resultSet1.getUnicodeStream(index)).andReturn(inputStream);
		
		this.replay();
		
		InputStream result = this.resultSet.getUnicodeStream(index);
		
		this.verify();
		
		assert result == inputStream;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getUnicodeStream(java.lang.String)
	 */
	@SuppressWarnings("deprecation")
	@Test(dataProvider = "string")
	@Deprecated
	public InputStream getUnicodeStream(String name) throws SQLException
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.resultSet1.getUnicodeStream(name)).andReturn(inputStream);
		
		this.replay();
		
		InputStream result = this.resultSet.getUnicodeStream(name);
		
		this.verify();
		
		assert result == inputStream;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getWarnings()
	 */
	@Test
	public SQLWarning getWarnings() throws SQLException
	{
		SQLWarning warning = new SQLWarning();
		
		EasyMock.expect(this.resultSet1.getWarnings()).andReturn(warning);
		
		this.replay();
		
		SQLWarning result = this.resultSet.getWarnings();
		
		this.verify();
		
		assert result == warning;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#insertRow()
	 */
	@Test
	public void insertRow() throws SQLException
	{
		EasyMock.expect(this.cluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.readLock);
		
		this.readLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		this.resultSet1.insertRow();
		this.resultSet2.insertRow();
		
		this.readLock.unlock();
		
		this.replay();
		
		this.resultSet.insertRow();
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#isAfterLast()
	 */
	@Test
	public boolean isAfterLast() throws SQLException
	{
		EasyMock.expect(this.resultSet1.isAfterLast()).andReturn(true);
		
		this.replay();
		
		boolean result = this.resultSet.isAfterLast();
		
		this.verify();
		
		assert result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#isBeforeFirst()
	 */
	@Test
	public boolean isBeforeFirst() throws SQLException
	{
		EasyMock.expect(this.resultSet1.isBeforeFirst()).andReturn(true);
		
		this.replay();
		
		boolean result = this.resultSet.isBeforeFirst();
		
		this.verify();
		
		assert result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#isFirst()
	 */
	@Test
	public boolean isFirst() throws SQLException
	{
		EasyMock.expect(this.resultSet1.isFirst()).andReturn(true);
		
		this.replay();
		
		boolean result = this.resultSet.isFirst();
		
		this.verify();
		
		assert result;
			
		return result;
	}

	/**
	 * @see java.sql.ResultSet#isLast()
	 */
	@Test
	public boolean isLast() throws SQLException
	{
		EasyMock.expect(this.resultSet1.isLast()).andReturn(true);
		
		this.replay();
		
		boolean result = this.resultSet.isLast();
		
		this.verify();
		
		assert result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#last()
	 */
	@Test
	public boolean last() throws SQLException
	{
		EasyMock.expect(this.resultSet1.last()).andReturn(true);
		EasyMock.expect(this.resultSet2.last()).andReturn(true);
		
		this.replay();
		
		boolean result = this.resultSet.last();
		
		this.verify();
		
		assert result;
				
		return result;
	}

	/**
	 * @see java.sql.ResultSet#moveToCurrentRow()
	 */
	@Test
	public void moveToCurrentRow() throws SQLException
	{
		this.resultSet1.moveToCurrentRow();
		this.resultSet2.moveToCurrentRow();
		
		this.replay();
		
		this.resultSet.moveToCurrentRow();
		
		this.verify();		
	}

	/**
	 * @see java.sql.ResultSet#moveToInsertRow()
	 */
	@Test
	public void moveToInsertRow() throws SQLException
	{
		this.resultSet1.moveToInsertRow();
		this.resultSet2.moveToInsertRow();
		
		this.replay();
		
		this.resultSet.moveToInsertRow();
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#next()
	 */
	@Test
	public boolean next() throws SQLException
	{
		EasyMock.expect(this.resultSet1.next()).andReturn(true);
		EasyMock.expect(this.resultSet2.next()).andReturn(true);
		
		this.replay();
		
		boolean result = this.resultSet.next();
		
		this.verify();
		
		assert result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#previous()
	 */
	@Test
	public boolean previous() throws SQLException
	{
		EasyMock.expect(this.resultSet1.previous()).andReturn(true);
		EasyMock.expect(this.resultSet2.previous()).andReturn(true);
		
		this.replay();
		
		boolean result = this.resultSet.previous();
		
		this.verify();
		
		assert result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#refreshRow()
	 */
	@Test
	public void refreshRow() throws SQLException
	{
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		this.resultSet1.refreshRow();
		this.resultSet2.refreshRow();
		
		this.replay();
		
		this.resultSet.refreshRow();
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#relative(int)
	 */
	@Test(dataProvider = "int")
	public boolean relative(int rows) throws SQLException
	{
		EasyMock.expect(this.resultSet1.relative(rows)).andReturn(true);
		EasyMock.expect(this.resultSet2.relative(rows)).andReturn(true);
		
		this.replay();

		boolean result = this.resultSet.relative(rows);
		
		this.verify();
		
		assert result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#rowDeleted()
	 */
	@Test
	public boolean rowDeleted() throws SQLException
	{
		EasyMock.expect(this.resultSet1.rowDeleted()).andReturn(true);
		
		this.replay();

		boolean result = this.resultSet.rowDeleted();
		
		this.verify();
		
		assert result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#rowInserted()
	 */
	@Test
	public boolean rowInserted() throws SQLException
	{
		EasyMock.expect(this.resultSet1.rowInserted()).andReturn(true);
		
		this.replay();

		boolean result = this.resultSet.rowInserted();
		
		this.verify();
		
		assert result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#rowUpdated()
	 */
	@Test
	public boolean rowUpdated() throws SQLException
	{
		EasyMock.expect(this.resultSet1.rowUpdated()).andReturn(true);
		
		this.replay();

		boolean result = this.resultSet.rowUpdated();
		
		this.verify();
		
		assert result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#setFetchDirection(int)
	 */
	@Test(dataProvider = "int")
	public void setFetchDirection(int direction) throws SQLException
	{
		this.resultSet1.setFetchDirection(direction);
		this.resultSet2.setFetchDirection(direction);
		
		this.replay();

		this.resultSet.setFetchDirection(direction);
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#setFetchSize(int)
	 */
	@Test(dataProvider = "int")
	public void setFetchSize(int rows) throws SQLException
	{
		this.resultSet1.setFetchSize(rows);
		this.resultSet2.setFetchSize(rows);
		
		this.replay();

		this.resultSet.setFetchSize(rows);
		
		this.verify();
	}

	@DataProvider(name = "int-array")
	Object[][] intArrayProvider()
	{
		return new Object[][] { new Object[] { 1, EasyMock.createMock(Array.class) } };
	}
		
	/**
	 * @see java.sql.ResultSet#updateArray(int, java.sql.Array)
	 */
	@Test(dataProvider = "int-array")
	public void updateArray(int index, Array value) throws SQLException
	{
		this.resultSet1.updateArray(index, value);
		this.resultSet2.updateArray(index, value);
		
		this.replay();

		this.resultSet.updateArray(index, value);
		
		this.verify();
	}

	@DataProvider(name = "string-array")
	Object[][] stringArrayProvider()
	{
		return new Object[][] { new Object[] { "", EasyMock.createMock(Array.class) } };
	}
	
	/**
	 * @see java.sql.ResultSet#updateArray(java.lang.String, java.sql.Array)
	 */
	@Test(dataProvider = "string-array")
	public void updateArray(String name, Array value) throws SQLException
	{
		this.resultSet1.updateArray(name, value);
		this.resultSet2.updateArray(name, value);
		
		this.replay();

		this.resultSet.updateArray(name, value);
		
		this.verify();
	}

	@DataProvider(name = "int-inputStream-int")
	Object[][] intInputStreamIntProvider()
	{
		return new Object[][] { new Object[] { 1, new ByteArrayInputStream(new byte[0]), 1 } };
	}
	
	/**
	 * @see java.sql.ResultSet#updateAsciiStream(int, java.io.InputStream, int)
	 */
	@Test(dataProvider = "int-inputStream-int")
	public void updateAsciiStream(int index, InputStream value, int length) throws SQLException
	{
		File file = new File("");
		InputStream input1 = new ByteArrayInputStream(new byte[0]);
		InputStream input2 = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.resultSet1.updateAsciiStream(index, input1, length);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.resultSet2.updateAsciiStream(index, input2, length);
		
		this.replay();

		this.resultSet.updateAsciiStream(index, value, length);
		
		this.verify();
	}

	@DataProvider(name = "string-inputStream-int")
	Object[][] stringInputStreamIntProvider()
	{
		return new Object[][] { new Object[] { "", new ByteArrayInputStream(new byte[0]), 1 } };
	}

	/**
	 * @see java.sql.ResultSet#updateAsciiStream(java.lang.String, java.io.InputStream, int)
	 */
	@Test(dataProvider = "string-inputStream-int")
	public void updateAsciiStream(String name, InputStream value, int length) throws SQLException
	{
		File file = new File("");
		InputStream input1 = new ByteArrayInputStream(new byte[0]);
		InputStream input2 = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.resultSet1.updateAsciiStream(name, input1, length);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.resultSet2.updateAsciiStream(name, input2, length);
		
		this.replay();

		this.resultSet.updateAsciiStream(name, value, length);
		
		this.verify();
	}

	@DataProvider(name = "int-bigDecimal")
	Object[][] intBigDecimalProvider()
	{
		return new Object[][] { new Object[] { 1, new BigDecimal(1.0) } };
	}

	/**
	 * @see java.sql.ResultSet#updateBigDecimal(int, java.math.BigDecimal)
	 */
	@Test(dataProvider = "int-bigDecimal")
	public void updateBigDecimal(int index, BigDecimal value) throws SQLException
	{
		this.resultSet1.updateBigDecimal(index, value);
		this.resultSet2.updateBigDecimal(index, value);
		
		this.replay();

		this.resultSet.updateBigDecimal(index, value);
		
		this.verify();
	}

	@DataProvider(name = "string-bigDecimal")
	Object[][] stringBigDecimalProvider()
	{
		return new Object[][] { new Object[] { "", new BigDecimal(1.0) } };
	}

	/**
	 * @see java.sql.ResultSet#updateBigDecimal(java.lang.String, java.math.BigDecimal)
	 */
	@Test(dataProvider = "string-bigDecimal")
	public void updateBigDecimal(String name, BigDecimal value) throws SQLException
	{
		this.resultSet1.updateBigDecimal(name, value);
		this.resultSet2.updateBigDecimal(name, value);
		
		this.replay();

		this.resultSet.updateBigDecimal(name, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateBinaryStream(int, java.io.InputStream, int)
	 */
	@Test(dataProvider = "int-inputStream-int")
	public void updateBinaryStream(int index, InputStream value, int length) throws SQLException
	{
		File file = new File("");
		InputStream input1 = new ByteArrayInputStream(new byte[0]);
		InputStream input2 = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.resultSet1.updateBinaryStream(index, input1, length);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.resultSet2.updateBinaryStream(index, input2, length);
		
		this.replay();

		this.resultSet.updateBinaryStream(index, value, length);
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateBinaryStream(java.lang.String, java.io.InputStream, int)
	 */
	@Test(dataProvider = "string-inputStream-int")
	public void updateBinaryStream(String name, InputStream value, int length) throws SQLException
	{
		File file = new File("");
		InputStream input1 = new ByteArrayInputStream(new byte[0]);
		InputStream input2 = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.resultSet1.updateBinaryStream(name, input1, length);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.resultSet2.updateBinaryStream(name, input2, length);
		
		this.replay();

		this.resultSet.updateBinaryStream(name, value, length);
		
		this.verify();
	}

	@DataProvider(name = "int-blob")
	Object[][] intBlobProvider() throws Exception
	{
		Map<Database, Blob> map = new TreeMap<Database, Blob>();
		
		map.put(this.database1, this.blob1);
		map.put(this.database2, this.blob2);
		
		Blob blob = ProxyFactory.createProxy(Blob.class, new BlobInvocationHandler(null, this.handler, null, map));
		
		return new Object[][] { new Object[] { 1, new MockBlob() }, new Object[] { 1, blob } };
	}

	/**
	 * @see java.sql.ResultSet#updateBlob(int, java.sql.Blob)
	 */
	@Test(dataProvider = "int-blob")
	public void updateBlob(int index, Blob value) throws SQLException
	{
		if (Proxy.isProxyClass(value.getClass()))
		{
			this.resultSet1.updateBlob(index, this.blob1);
			this.resultSet2.updateBlob(index, this.blob2);
		}
		else
		{
			this.resultSet1.updateBlob(EasyMock.eq(index), EasyMock.isA(SerialBlob.class));
			this.resultSet2.updateBlob(EasyMock.eq(index), EasyMock.isA(SerialBlob.class));
		}
		
		this.replay();

		this.resultSet.updateBlob(index, value);
		
		this.verify();
	}

	@DataProvider(name = "string-blob")
	Object[][] stringBlobProvider() throws Exception
	{
		Map<Database, Blob> map = new TreeMap<Database, Blob>();
		
		map.put(this.database1, this.blob1);
		map.put(this.database2, this.blob2);
		
		Blob blob = ProxyFactory.createProxy(Blob.class, new BlobInvocationHandler(null, this.handler, null, map));
		
		return new Object[][] { new Object[] { "", new MockBlob() }, new Object[] { "", blob } };
	}

	/**
	 * @see java.sql.ResultSet#updateBlob(java.lang.String, java.sql.Blob)
	 */
	@Test(dataProvider = "string-blob")
	public void updateBlob(String name, Blob value) throws SQLException
	{
		if (Proxy.isProxyClass(value.getClass()))
		{
			this.resultSet1.updateBlob(name, this.blob1);
			this.resultSet2.updateBlob(name, this.blob2);
		}
		else
		{
			this.resultSet1.updateBlob(EasyMock.eq(name), EasyMock.isA(SerialBlob.class));
			this.resultSet2.updateBlob(EasyMock.eq(name), EasyMock.isA(SerialBlob.class));
		}
		
		this.replay();

		this.resultSet.updateBlob(name, value);
		
		this.verify();
	}

	@DataProvider(name = "int-boolean")
	Object[][] intBooleanProvider()
	{
		return new Object[][] { new Object[] { 1, true } };
	}

	/**
	 * @see java.sql.ResultSet#updateBoolean(int, boolean)
	 */
	@Test(dataProvider = "int-boolean")
	public void updateBoolean(int index, boolean value) throws SQLException
	{
		this.resultSet1.updateBoolean(index, value);
		this.resultSet2.updateBoolean(index, value);
		
		this.replay();

		this.resultSet.updateBoolean(index, value);
		
		this.verify();
	}

	@DataProvider(name = "string-boolean")
	Object[][] stringBooleanProvider()
	{
		return new Object[][] { new Object[] { "", true } };
	}

	/**
	 * @see java.sql.ResultSet#updateBoolean(java.lang.String, boolean)
	 */
	@Test(dataProvider = "string-boolean")
	public void updateBoolean(String name, boolean value) throws SQLException
	{
		this.resultSet1.updateBoolean(name, value);
		this.resultSet2.updateBoolean(name, value);
		
		this.replay();

		this.resultSet.updateBoolean(name, value);
		
		this.verify();
	}

	@DataProvider(name = "int-byte")
	Object[][] intByteProvider()
	{
		return new Object[][] { new Object[] { 1, Integer.valueOf(1).byteValue() } };
	}

	/**
	 * @see java.sql.ResultSet#updateByte(int, byte)
	 */
	@Test(dataProvider = "int-byte")
	public void updateByte(int index, byte value) throws SQLException
	{
		this.resultSet1.updateByte(index, value);
		this.resultSet2.updateByte(index, value);
		
		this.replay();

		this.resultSet.updateByte(index, value);
		
		this.verify();
	}

	@DataProvider(name = "string-byte")
	Object[][] stringByteProvider()
	{
		return new Object[][] { new Object[] { "", Integer.valueOf(1).byteValue() } };
	}

	/**
	 * @see java.sql.ResultSet#updateByte(java.lang.String, byte)
	 */
	@Test(dataProvider = "string-byte")
	public void updateByte(String name, byte value) throws SQLException
	{
		this.resultSet1.updateByte(name, value);
		this.resultSet2.updateByte(name, value);
		
		this.replay();

		this.resultSet.updateByte(name, value);
		
		this.verify();
	}

	@DataProvider(name = "int-bytes")
	Object[][] intBytesProvider()
	{
		return new Object[][] { new Object[] { 1, new byte[0] } };
	}

	/**
	 * @see java.sql.ResultSet#updateBytes(int, byte[])
	 */
	@Test(dataProvider = "int-bytes")
	public void updateBytes(int index, byte[] value) throws SQLException
	{
		this.resultSet1.updateBytes(index, value);
		this.resultSet2.updateBytes(index, value);
		
		this.replay();

		this.resultSet.updateBytes(index, value);
		
		this.verify();
	}

	@DataProvider(name = "string-bytes")
	Object[][] stringBytesProvider()
	{
		return new Object[][] { new Object[] { "", new byte[0] } };
	}

	/**
	 * @see java.sql.ResultSet#updateBytes(java.lang.String, byte[])
	 */
	@Test(dataProvider = "string-bytes")
	public void updateBytes(String name, byte[] value) throws SQLException
	{
		this.resultSet1.updateBytes(name, value);
		this.resultSet2.updateBytes(name, value);
		
		this.replay();

		this.resultSet.updateBytes(name, value);
		
		this.verify();
	}

	@DataProvider(name = "int-reader-int")
	Object[][] intReaderIntProvider()
	{
		return new Object[][] { new Object[] { 1, new CharArrayReader(new char[0]), 0 } };
	}

	/**
	 * @see java.sql.ResultSet#updateCharacterStream(int, java.io.Reader, int)
	 */
	@Test(dataProvider = "int-reader-int")
	public void updateCharacterStream(int index, Reader value, int length) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new CharArrayReader(new char[0]);
		Reader reader2 = new CharArrayReader(new char[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.resultSet1.updateCharacterStream(index, reader1, length);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.resultSet2.updateCharacterStream(index, reader2, length);
		
		this.replay();

		this.resultSet.updateCharacterStream(index, value, length);
		
		this.verify();
	}

	@DataProvider(name = "string-reader-int")
	Object[][] stringReaderIntProvider()
	{
		return new Object[][] { new Object[] { "", new CharArrayReader(new char[0]), 0 } };
	}

	/**
	 * @see java.sql.ResultSet#updateCharacterStream(java.lang.String, java.io.Reader, int)
	 */
	@Test(dataProvider = "string-reader-int")
	public void updateCharacterStream(String name, Reader value, int length) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new CharArrayReader(new char[0]);
		Reader reader2 = new CharArrayReader(new char[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.resultSet1.updateCharacterStream(name, reader1, length);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.resultSet2.updateCharacterStream(name, reader2, length);
		
		this.replay();

		this.resultSet.updateCharacterStream(name, value, length);
		
		this.verify();
	}

	@DataProvider(name = "int-clob")
	Object[][] intClobProvider() throws Exception
	{
		Map<Database, Clob> map = new TreeMap<Database, Clob>();
		
		map.put(this.database1, this.clob1);
		map.put(this.database2, this.clob2);
		
		Clob clob = ProxyFactory.createProxy(Clob.class, new ClobInvocationHandler(null, this.handler, null, map));
		
		return new Object[][] { new Object[] { 1, new MockClob() }, new Object[] { 1, clob } };
	}

	/**
	 * @see java.sql.ResultSet#updateClob(int, java.sql.Clob)
	 */
	@Test(dataProvider = "int-clob")
	public void updateClob(int index, Clob value) throws SQLException
	{
		if (Proxy.isProxyClass(value.getClass()))
		{
			this.resultSet1.updateClob(index, this.clob1);
			this.resultSet2.updateClob(index, this.clob2);
		}
		else
		{
			this.resultSet1.updateClob(EasyMock.eq(index), EasyMock.isA(SerialClob.class));
			this.resultSet2.updateClob(EasyMock.eq(index), EasyMock.isA(SerialClob.class));
		}
		
		this.replay();

		this.resultSet.updateClob(index, value);
		
		this.verify();
	}

	@DataProvider(name = "string-clob")
	Object[][] stringClobProvider() throws Exception
	{
		Map<Database, Clob> map = new TreeMap<Database, Clob>();
		
		map.put(this.database1, this.clob1);
		map.put(this.database2, this.clob2);
		
		Clob clob = ProxyFactory.createProxy(Clob.class, new ClobInvocationHandler(null, this.handler, null, map));
		
		return new Object[][] { new Object[] { "", new MockClob() }, new Object[] { "", clob } };
	}

	/**
	 * @see java.sql.ResultSet#updateClob(java.lang.String, java.sql.Clob)
	 */
	@Test(dataProvider = "string-clob")
	public void updateClob(String name, Clob value) throws SQLException
	{
		if (Proxy.isProxyClass(value.getClass()))
		{
			this.resultSet1.updateClob(name, this.clob1);
			this.resultSet2.updateClob(name, this.clob2);
		}
		else
		{
			this.resultSet1.updateClob(EasyMock.eq(name), EasyMock.isA(SerialClob.class));
			this.resultSet2.updateClob(EasyMock.eq(name), EasyMock.isA(SerialClob.class));
		}
		
		this.replay();

		this.resultSet.updateClob(name, value);
		
		this.verify();
	}

	@DataProvider(name = "int-date")
	Object[][] intDateProvider()
	{
		return new Object[][] { new Object[] { 1, new Date(System.currentTimeMillis()) } };
	}

	/**
	 * @see java.sql.ResultSet#updateDate(int, java.sql.Date)
	 */
	@Test(dataProvider = "int-date")
	public void updateDate(int index, Date value) throws SQLException
	{
		this.resultSet1.updateDate(index, value);
		this.resultSet2.updateDate(index, value);
		
		this.replay();

		this.resultSet.updateDate(index, value);
		
		this.verify();
	}

	@DataProvider(name = "string-date")
	Object[][] stringDateProvider()
	{
		return new Object[][] { new Object[] { "", new Date(System.currentTimeMillis()) } };
	}

	/**
	 * @see java.sql.ResultSet#updateDate(java.lang.String, java.sql.Date)
	 */
	@Test(dataProvider = "string-date")
	public void updateDate(String name, Date value) throws SQLException
	{
		this.resultSet1.updateDate(name, value);
		this.resultSet2.updateDate(name, value);
		
		this.replay();

		this.resultSet.updateDate(name, value);
		
		this.verify();
	}

	@DataProvider(name = "int-double")
	Object[][] intDoubleProvider()
	{
		return new Object[][] { new Object[] { 1, 1.0 } };
	}

	/**
	 * @see java.sql.ResultSet#updateDouble(int, double)
	 */
	@Test(dataProvider = "int-double")
	public void updateDouble(int index, double value) throws SQLException
	{
		this.resultSet1.updateDouble(index, value);
		this.resultSet2.updateDouble(index, value);
		
		this.replay();

		this.resultSet.updateDouble(index, value);
		
		this.verify();
	}

	@DataProvider(name = "string-double")
	Object[][] stringDoubleProvider()
	{
		return new Object[][] { new Object[] { "", 1.0 } };
	}

	/**
	 * @see java.sql.ResultSet#updateDouble(java.lang.String, double)
	 */
	@Test(dataProvider = "string-double")
	public void updateDouble(String name, double value) throws SQLException
	{
		this.resultSet1.updateDouble(name, value);
		this.resultSet2.updateDouble(name, value);
		
		this.replay();

		this.resultSet.updateDouble(name, value);
		
		this.verify();
	}

	@DataProvider(name = "int-float")
	Object[][] intFloatProvider()
	{
		return new Object[][] { new Object[] { 1, 1.0F } };
	}

	/**
	 * @see java.sql.ResultSet#updateFloat(int, float)
	 */
	@Test(dataProvider = "int-float")
	public void updateFloat(int index, float value) throws SQLException
	{
		this.resultSet1.updateFloat(index, value);
		this.resultSet2.updateFloat(index, value);
		
		this.replay();

		this.resultSet.updateFloat(index, value);
		
		this.verify();
	}

	@DataProvider(name = "string-float")
	Object[][] stringFloatProvider()
	{
		return new Object[][] { new Object[] { "", 1.0F } };
	}

	/**
	 * @see java.sql.ResultSet#updateFloat(java.lang.String, float)
	 */
	@Test(dataProvider = "string-float")
	public void updateFloat(String name, float value) throws SQLException
	{
		this.resultSet1.updateFloat(name, value);
		this.resultSet2.updateFloat(name, value);
		
		this.replay();

		this.resultSet.updateFloat(name, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateInt(int, int)
	 */
	@Test(dataProvider = "int-int")
	public void updateInt(int index, int value) throws SQLException
	{
		this.resultSet1.updateInt(index, value);
		this.resultSet2.updateInt(index, value);
		
		this.replay();

		this.resultSet.updateInt(index, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateInt(java.lang.String, int)
	 */
	@Test(dataProvider = "string-int")
	public void updateInt(String name, int value) throws SQLException
	{
		this.resultSet1.updateInt(name, value);
		this.resultSet2.updateInt(name, value);
		
		this.replay();

		this.resultSet.updateInt(name, value);
		
		this.verify();
	}

	@DataProvider(name = "int-long")
	Object[][] intLongProvider()
	{
		return new Object[][] { new Object[] { 1, 1L } };
	}

	/**
	 * @see java.sql.ResultSet#updateLong(int, long)
	 */
	@Test(dataProvider = "int-long")
	public void updateLong(int index, long value) throws SQLException
	{
		this.resultSet1.updateLong(index, value);
		this.resultSet2.updateLong(index, value);
		
		this.replay();

		this.resultSet.updateLong(index, value);
		
		this.verify();
	}

	@DataProvider(name = "string-long")
	Object[][] stringLongProvider()
	{
		return new Object[][] { new Object[] { "", 1L } };
	}

	/**
	 * @see java.sql.ResultSet#updateLong(java.lang.String, long)
	 */
	@Test(dataProvider = "string-long")
	public void updateLong(String name, long value) throws SQLException
	{
		this.resultSet1.updateLong(name, value);
		this.resultSet2.updateLong(name, value);
		
		this.replay();

		this.resultSet.updateLong(name, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateNull(int)
	 */
	@Test(dataProvider = "int")
	public void updateNull(int index) throws SQLException
	{
		this.resultSet1.updateNull(index);
		this.resultSet2.updateNull(index);
		
		this.replay();

		this.resultSet.updateNull(index);
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateNull(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public void updateNull(String name) throws SQLException
	{
		this.resultSet1.updateNull(name);
		this.resultSet2.updateNull(name);
		
		this.replay();

		this.resultSet.updateNull(name);
		
		this.verify();
	}

	@DataProvider(name = "int-object")
	Object[][] intObjectProvider()
	{
		return new Object[][] { new Object[] { 1, new Object() } };
	}

	/**
	 * @see java.sql.ResultSet#updateObject(int, java.lang.Object)
	 */
	@Test(dataProvider = "int-object")
	public void updateObject(int index, Object value) throws SQLException
	{
		this.resultSet1.updateObject(index, value);
		this.resultSet2.updateObject(index, value);
		
		this.replay();

		this.resultSet.updateObject(index, value);
		
		this.verify();
	}

	@DataProvider(name = "string-object")
	Object[][] stringObjectProvider()
	{
		return new Object[][] { new Object[] { "", new Object() } };
	}

	/**
	 * @see java.sql.ResultSet#updateObject(java.lang.String, java.lang.Object)
	 */
	@Test(dataProvider = "string-object")
	public void updateObject(String name, Object value) throws SQLException
	{
		this.resultSet1.updateObject(name, value);
		this.resultSet2.updateObject(name, value);
		
		this.replay();

		this.resultSet.updateObject(name, value);
		
		this.verify();
	}

	@DataProvider(name = "int-object-int")
	Object[][] intObjectIntProvider()
	{
		return new Object[][] { new Object[] { 1, new Object(), 1 } };
	}

	/**
	 * @see java.sql.ResultSet#updateObject(int, java.lang.Object, int)
	 */
	@Test(dataProvider = "int-object-int")
	public void updateObject(int index, Object value, int scale) throws SQLException
	{
		this.resultSet1.updateObject(index, value, scale);
		this.resultSet2.updateObject(index, value, scale);
		
		this.replay();

		this.resultSet.updateObject(index, value, scale);
		
		this.verify();
	}

	@DataProvider(name = "string-object-int")
	Object[][] stringObjectIntProvider()
	{
		return new Object[][] { new Object[] { "", new Object(), 1 } };
	}

	/**
	 * @see java.sql.ResultSet#updateObject(java.lang.String, java.lang.Object, int)
	 */
	@Test(dataProvider = "string-object-int")
	public void updateObject(String name, Object value, int scale) throws SQLException
	{
		this.resultSet1.updateObject(name, value, scale);
		this.resultSet2.updateObject(name, value, scale);
		
		this.replay();

		this.resultSet.updateObject(name, value, scale);
		
		this.verify();
	}

	@DataProvider(name = "int-ref")
	Object[][] intRefProvider()
	{
		return new Object[][] { new Object[] { 1, EasyMock.createMock(Ref.class) } };
	}

	/**
	 * @see java.sql.ResultSet#updateRef(int, java.sql.Ref)
	 */
	@Test(dataProvider = "int-ref")
	public void updateRef(int index, Ref value) throws SQLException
	{
		this.resultSet1.updateRef(index, value);
		this.resultSet2.updateRef(index, value);
		
		this.replay();

		this.resultSet.updateRef(index, value);
		
		this.verify();
	}

	@DataProvider(name = "string-ref")
	Object[][] stringRefProvider()
	{
		return new Object[][] { new Object[] { "", EasyMock.createMock(Ref.class) } };
	}

	/**
	 * @see java.sql.ResultSet#updateRef(java.lang.String, java.sql.Ref)
	 */
	@Test(dataProvider = "string-ref")
	public void updateRef(String name, Ref value) throws SQLException
	{
		this.resultSet1.updateRef(name, value);
		this.resultSet2.updateRef(name, value);
		
		this.replay();

		this.resultSet.updateRef(name, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateRow()
	 */
	@Test
	public void updateRow() throws SQLException
	{
		EasyMock.expect(this.cluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.readLock);
		
		this.readLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);

		this.resultSet1.updateRow();
		this.resultSet2.updateRow();
		
		this.readLock.unlock();
		
		this.replay();
		
		this.resultSet.updateRow();
		
		this.verify();
	}

	@DataProvider(name = "int-short")
	Object[][] intShortProvider()
	{
		return new Object[][] { new Object[] { 1, Integer.valueOf(1).shortValue() } };
	}

	/**
	 * @see java.sql.ResultSet#updateShort(int, short)
	 */
	@Test(dataProvider = "int-short")
	public void updateShort(int index, short value) throws SQLException
	{
		this.resultSet1.updateShort(index, value);
		this.resultSet2.updateShort(index, value);
		
		this.replay();

		this.resultSet.updateShort(index, value);
		
		this.verify();
	}

	@DataProvider(name = "string-short")
	Object[][] stringShortProvider()
	{
		return new Object[][] { new Object[] { "", Integer.valueOf(1).shortValue() } };
	}

	/**
	 * @see java.sql.ResultSet#updateShort(java.lang.String, short)
	 */
	@Test(dataProvider = "string-short")
	public void updateShort(String name, short value) throws SQLException
	{
		this.resultSet1.updateShort(name, value);
		this.resultSet2.updateShort(name, value);
		
		this.replay();

		this.resultSet.updateShort(name, value);
		
		this.verify();
	}

	@DataProvider(name = "int-string")
	Object[][] intStringProvider()
	{
		return new Object[][] { new Object[] { 1, "" } };
	}

	/**
	 * @see java.sql.ResultSet#updateString(int, java.lang.String)
	 */
	@Test(dataProvider = "int-string")
	public void updateString(int index, String value) throws SQLException
	{
		this.resultSet1.updateString(index, value);
		this.resultSet2.updateString(index, value);
		
		this.replay();

		this.resultSet.updateString(index, value);
		
		this.verify();
	}

	@DataProvider(name = "string-string")
	Object[][] stringStringProvider()
	{
		return new Object[][] { new Object[] { "", "" } };
	}

	/**
	 * @see java.sql.ResultSet#updateString(java.lang.String, java.lang.String)
	 */
	@Test(dataProvider = "string-string")
	public void updateString(String name, String value) throws SQLException
	{
		this.resultSet1.updateString(name, value);
		this.resultSet2.updateString(name, value);
		
		this.replay();

		this.resultSet.updateString(name, value);
		
		this.verify();
	}

	@DataProvider(name = "int-time")
	Object[][] intTimeProvider()
	{
		return new Object[][] { new Object[] { 1, new Time(System.currentTimeMillis()) } };
	}

	/**
	 * @see java.sql.ResultSet#updateTime(int, java.sql.Time)
	 */
	@Test(dataProvider = "int-time")
	public void updateTime(int index, Time value) throws SQLException
	{
		this.resultSet1.updateTime(index, value);
		this.resultSet2.updateTime(index, value);
		
		this.replay();

		this.resultSet.updateTime(index, value);
		
		this.verify();
	}

	@DataProvider(name = "string-time")
	Object[][] stringTimeProvider()
	{
		return new Object[][] { new Object[] { "", new Time(System.currentTimeMillis()) } };
	}

	/**
	 * @see java.sql.ResultSet#updateTime(java.lang.String, java.sql.Time)
	 */
	@Test(dataProvider = "string-time")
	public void updateTime(String name, Time value) throws SQLException
	{
		this.resultSet1.updateTime(name, value);
		this.resultSet2.updateTime(name, value);
		
		this.replay();

		this.resultSet.updateTime(name, value);
		
		this.verify();
	}

	@DataProvider(name = "int-timestamp")
	Object[][] intTimestampProvider()
	{
		return new Object[][] { new Object[] { 1, new Timestamp(System.currentTimeMillis()) } };
	}

	/**
	 * @see java.sql.ResultSet#updateTimestamp(int, java.sql.Timestamp)
	 */
	@Test(dataProvider = "int-timestamp")
	public void updateTimestamp(int index, Timestamp value) throws SQLException
	{
		this.resultSet1.updateTimestamp(index, value);
		this.resultSet2.updateTimestamp(index, value);
		
		this.replay();

		this.resultSet.updateTimestamp(index, value);
		
		this.verify();
	}

	@DataProvider(name = "string-timestamp")
	Object[][] stringTimestampProvider()
	{
		return new Object[][] { new Object[] { "", new Timestamp(System.currentTimeMillis()) } };
	}

	/**
	 * @see java.sql.ResultSet#updateTimestamp(java.lang.String, java.sql.Timestamp)
	 */
	@Test(dataProvider = "string-timestamp")
	public void updateTimestamp(String name, Timestamp value) throws SQLException
	{
		this.resultSet1.updateTimestamp(name, value);
		this.resultSet2.updateTimestamp(name, value);
		
		this.replay();

		this.resultSet.updateTimestamp(name, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#wasNull()
	 */
	@Test
	public boolean wasNull() throws SQLException
	{
		EasyMock.expect(this.resultSet1.wasNull()).andReturn(true);
		
		this.replay();

		boolean result = this.resultSet.wasNull();
		
		this.verify();
		
		assert result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getHoldability()
	 */
	@Test
	public int getHoldability() throws SQLException
	{
		EasyMock.expect(this.resultSet1.getHoldability()).andReturn(1);
		
		this.replay();
		
		int result = this.resultSet.getHoldability();
		
		this.verify();
		
		assert result == 1 : result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getNCharacterStream(int)
	 */
	@Test(dataProvider = "int")
	public Reader getNCharacterStream(int index) throws SQLException
	{
		Reader reader = new StringReader("");
		
		EasyMock.expect(this.resultSet1.getNCharacterStream(index)).andReturn(reader);
		
		this.replay();
		
		Reader result = this.resultSet.getNCharacterStream(index);
		
		this.verify();
		
		assert result == reader;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getNCharacterStream(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public Reader getNCharacterStream(String name) throws SQLException
	{
		Reader reader = new StringReader("");
		
		EasyMock.expect(this.resultSet1.getNCharacterStream(name)).andReturn(reader);
		
		this.replay();
		
		Reader result = this.resultSet.getNCharacterStream(name);
		
		this.verify();
		
		assert result == reader;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getNClob(int)
	 */
	public NClob getNClob(int index) throws SQLException
	{
		NClob clob = EasyMock.createMock(NClob.class);
		
		EasyMock.expect(this.resultSet1.getNClob(index)).andReturn(clob);
		
		this.replay();
		
		NClob result = this.resultSet.getNClob(index);
		
		this.verify();
		
		assert result == clob;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getNClob(java.lang.String)
	 */
	public NClob getNClob(String name) throws SQLException
	{
		NClob clob = EasyMock.createMock(NClob.class);
		
		EasyMock.expect(this.resultSet1.getNClob(name)).andReturn(clob);
		
		this.replay();
		
		NClob result = this.resultSet.getNClob(name);
		
		this.verify();
		
		assert result == clob;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getNString(int)
	 */
	@Test(dataProvider = "int")
	public String getNString(int index) throws SQLException
	{
		EasyMock.expect(this.resultSet1.getNString(index)).andReturn("");
		
		this.replay();
		
		String result = this.resultSet.getNString(index);
		
		this.verify();
		
		assert result.equals("") : result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getNString(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public String getNString(String name) throws SQLException
	{
		EasyMock.expect(this.resultSet1.getNString(name)).andReturn("");
		
		this.replay();
		
		String result = this.resultSet.getNString(name);
		
		this.verify();
		
		assert result.equals("") : result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getRowId(int)
	 */
	@Test(dataProvider = "int")
	public RowId getRowId(int index) throws SQLException
	{
		RowId rowId = EasyMock.createMock(RowId.class);
		
		EasyMock.expect(this.resultSet1.getRowId(index)).andReturn(rowId);
		
		this.replay();
		
		RowId result = this.resultSet.getRowId(index);
		
		this.verify();
		
		assert result == rowId;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getRowId(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public RowId getRowId(String name) throws SQLException
	{
		RowId rowId = EasyMock.createMock(RowId.class);
		
		EasyMock.expect(this.resultSet1.getRowId(name)).andReturn(rowId);
		
		this.replay();
		
		RowId result = this.resultSet.getRowId(name);
		
		this.verify();
		
		assert result == rowId;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getSQLXML(int)
	 */
	@Test(dataProvider = "int")
	public SQLXML getSQLXML(int index) throws SQLException
	{
		SQLXML xml = EasyMock.createMock(SQLXML.class);
		
		EasyMock.expect(this.resultSet1.getSQLXML(index)).andReturn(xml);
		
		this.replay();
		
		SQLXML result = this.resultSet.getSQLXML(index);
		
		this.verify();
		
		assert result == xml;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getSQLXML(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public SQLXML getSQLXML(String name) throws SQLException
	{
		SQLXML xml = EasyMock.createMock(SQLXML.class);
		
		EasyMock.expect(this.resultSet1.getSQLXML(name)).andReturn(xml);
		
		this.replay();
		
		SQLXML result = this.resultSet.getSQLXML(name);
		
		this.verify();
		
		assert result == xml;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#isClosed()
	 */
	@Test
	public boolean isClosed() throws SQLException
	{
		EasyMock.expect(this.resultSet1.isClosed()).andReturn(true);
		
		this.replay();
		
		boolean result = this.resultSet.isClosed();
		
		this.verify();
		
		assert result;
		
		return result;
	}

	@DataProvider(name = "int-inputStream")
	Object[][] intInputStreamProvider()
	{
		return new Object[][] { new Object[] { 1, new ByteArrayInputStream(new byte[0]) } };
	}
	
	/**
	 * @see java.sql.ResultSet#updateAsciiStream(int, java.io.InputStream)
	 */
	@Test(dataProvider = "int-inputStream")
	public void updateAsciiStream(int index, InputStream value) throws SQLException
	{
		File file = new File("");
		InputStream input1 = new ByteArrayInputStream(new byte[0]);
		InputStream input2 = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.resultSet1.updateAsciiStream(index, input1);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.resultSet2.updateAsciiStream(index, input2);
		
		this.replay();

		this.resultSet.updateAsciiStream(index, value);
		
		this.verify();
	}

	@DataProvider(name = "string-inputStream")
	Object[][] stringInputStreamProvider()
	{
		return new Object[][] { new Object[] { "", new ByteArrayInputStream(new byte[0]) } };
	}
	
	/**
	 * @see java.sql.ResultSet#updateAsciiStream(java.lang.String, java.io.InputStream)
	 */
	@Test(dataProvider = "string-inputStream")
	public void updateAsciiStream(String name, InputStream value) throws SQLException
	{
		File file = new File("");
		InputStream input1 = new ByteArrayInputStream(new byte[0]);
		InputStream input2 = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.resultSet1.updateAsciiStream(name, input1);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.resultSet2.updateAsciiStream(name, input2);
		
		this.replay();

		this.resultSet.updateAsciiStream(name, value);
		
		this.verify();
	}

	@DataProvider(name = "int-inputStream-long")
	Object[][] intInputStreamLongProvider()
	{
		return new Object[][] { new Object[] { 1, new ByteArrayInputStream(new byte[0]), 1L } };
	}
	
	/**
	 * @see java.sql.ResultSet#updateAsciiStream(int, java.io.InputStream, long)
	 */
	@Test(dataProvider = "int-inputStream-long")
	public void updateAsciiStream(int index, InputStream value, long length) throws SQLException
	{
		File file = new File("");
		InputStream input1 = new ByteArrayInputStream(new byte[0]);
		InputStream input2 = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.resultSet1.updateAsciiStream(index, input1, length);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.resultSet2.updateAsciiStream(index, input2, length);
		
		this.replay();

		this.resultSet.updateAsciiStream(index, value, length);
		
		this.verify();
	}

	@DataProvider(name = "string-inputStream-long")
	Object[][] stringInputStreamLongProvider()
	{
		return new Object[][] { new Object[] { "", new ByteArrayInputStream(new byte[0]), 1L } };
	}
	
	/**
	 * @see java.sql.ResultSet#updateAsciiStream(java.lang.String, java.io.InputStream, long)
	 */
	@Test(dataProvider = "string-inputStream-long")
	public void updateAsciiStream(String name, InputStream value, long length) throws SQLException
	{
		File file = new File("");
		InputStream input1 = new ByteArrayInputStream(new byte[0]);
		InputStream input2 = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.resultSet1.updateAsciiStream(name, input1, length);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.resultSet2.updateAsciiStream(name, input2, length);
		
		this.replay();

		this.resultSet.updateAsciiStream(name, value, length);
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateBinaryStream(int, java.io.InputStream)
	 */
	@Test(dataProvider = "int-inputStream")
	public void updateBinaryStream(int index, InputStream value) throws SQLException
	{
		File file = new File("");
		InputStream input1 = new ByteArrayInputStream(new byte[0]);
		InputStream input2 = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.resultSet1.updateBinaryStream(index, input1);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.resultSet2.updateBinaryStream(index, input2);
		
		this.replay();

		this.resultSet.updateBinaryStream(index, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateBinaryStream(java.lang.String, java.io.InputStream)
	 */
	@Test(dataProvider = "string-inputStream")
	public void updateBinaryStream(String name, InputStream value) throws SQLException
	{
		File file = new File("");
		InputStream input1 = new ByteArrayInputStream(new byte[0]);
		InputStream input2 = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.resultSet1.updateBinaryStream(name, input1);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.resultSet2.updateBinaryStream(name, input2);
		
		this.replay();

		this.resultSet.updateBinaryStream(name, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateBinaryStream(int, java.io.InputStream, long)
	 */
	@Test(dataProvider = "int-inputStream-long")
	public void updateBinaryStream(int index, InputStream value, long length) throws SQLException
	{
		File file = new File("");
		InputStream input1 = new ByteArrayInputStream(new byte[0]);
		InputStream input2 = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.resultSet1.updateBinaryStream(index, input1, length);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.resultSet2.updateBinaryStream(index, input2, length);
		
		this.replay();

		this.resultSet.updateBinaryStream(index, value, length);
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateBinaryStream(java.lang.String, java.io.InputStream, long)
	 */
	@Test(dataProvider = "string-inputStream-long")
	public void updateBinaryStream(String name, InputStream value, long length) throws SQLException
	{
		File file = new File("");
		InputStream input1 = new ByteArrayInputStream(new byte[0]);
		InputStream input2 = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.resultSet1.updateBinaryStream(name, input1, length);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.resultSet2.updateBinaryStream(name, input2, length);
		
		this.replay();

		this.resultSet.updateBinaryStream(name, value, length);
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateBlob(int, java.io.InputStream)
	 */
	@Test(dataProvider = "int-inputStream")
	public void updateBlob(int index, InputStream value) throws SQLException
	{
		File file = new File("");
		InputStream input1 = new ByteArrayInputStream(new byte[0]);
		InputStream input2 = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.resultSet1.updateBlob(index, input1);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.resultSet2.updateBlob(index, input2);
		
		this.replay();

		this.resultSet.updateBlob(index, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateBlob(java.lang.String, java.io.InputStream)
	 */
	@Test(dataProvider = "string-inputStream")
	public void updateBlob(String name, InputStream value) throws SQLException
	{
		File file = new File("");
		InputStream input1 = new ByteArrayInputStream(new byte[0]);
		InputStream input2 = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.resultSet1.updateBlob(name, input1);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.resultSet2.updateBlob(name, input2);
		
		this.replay();

		this.resultSet.updateBlob(name, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateBlob(int, java.io.InputStream, long)
	 */
	@Test(dataProvider = "int-inputStream-long")
	public void updateBlob(int index, InputStream value, long length) throws SQLException
	{
		File file = new File("");
		InputStream input1 = new ByteArrayInputStream(new byte[0]);
		InputStream input2 = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.resultSet1.updateBlob(index, input1, length);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.resultSet2.updateBlob(index, input2, length);
		
		this.replay();

		this.resultSet.updateBlob(index, value, length);
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateBlob(java.lang.String, java.io.InputStream, long)
	 */
	@Test(dataProvider = "string-inputStream-long")
	public void updateBlob(String name, InputStream value, long length) throws SQLException
	{
		File file = new File("");
		InputStream input1 = new ByteArrayInputStream(new byte[0]);
		InputStream input2 = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.resultSet1.updateBlob(name, input1, length);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.resultSet2.updateBlob(name, input2, length);
		
		this.replay();

		this.resultSet.updateBlob(name, value, length);
		
		this.verify();
	}

	@DataProvider(name = "int-reader")
	Object[][] intReaderProvider()
	{
		return new Object[][] { new Object[] { 1, new StringReader("") } };
	}
	
	/**
	 * @see java.sql.ResultSet#updateCharacterStream(int, java.io.Reader)
	 */
	@Test(dataProvider = "int-reader")
	public void updateCharacterStream(int index, Reader value) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new StringReader("");
		Reader reader2 = new StringReader("");
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.resultSet1.updateCharacterStream(index, reader1);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.resultSet2.updateCharacterStream(index, reader2);
		
		this.replay();

		this.resultSet.updateCharacterStream(index, value);
		
		this.verify();
	}

	@DataProvider(name = "string-reader")
	Object[][] stringReaderProvider()
	{
		return new Object[][] { new Object[] { "", new StringReader("") } };
	}
	
	/**
	 * @see java.sql.ResultSet#updateCharacterStream(java.lang.String, java.io.Reader)
	 */
	@Test(dataProvider = "string-reader")
	public void updateCharacterStream(String name, Reader value) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new StringReader("");
		Reader reader2 = new StringReader("");
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.resultSet1.updateCharacterStream(name, reader1);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.resultSet2.updateCharacterStream(name, reader2);
		
		this.replay();

		this.resultSet.updateCharacterStream(name, value);
		
		this.verify();
	}

	@DataProvider(name = "int-reader-long")
	Object[][] intReaderLongProvider()
	{
		return new Object[][] { new Object[] { 1, new StringReader(""), 1L } };
	}
	
	/**
	 * @see java.sql.ResultSet#updateCharacterStream(int, java.io.Reader, long)
	 */
	@Test(dataProvider = "int-reader-long")
	public void updateCharacterStream(int index, Reader value, long length) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new StringReader("");
		Reader reader2 = new StringReader("");
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.resultSet1.updateCharacterStream(index, reader1, length);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.resultSet2.updateCharacterStream(index, reader2, length);
		
		this.replay();

		this.resultSet.updateCharacterStream(index, value, length);
		
		this.verify();
	}

	@DataProvider(name = "string-reader-long")
	Object[][] stringReaderLongProvider()
	{
		return new Object[][] { new Object[] { "", new StringReader(""), 1L } };
	}
	
	/**
	 * @see java.sql.ResultSet#updateCharacterStream(java.lang.String, java.io.Reader, long)
	 */
	@Test(dataProvider = "string-reader-long")
	public void updateCharacterStream(String name, Reader value, long length) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new StringReader("");
		Reader reader2 = new StringReader("");
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.resultSet1.updateCharacterStream(name, reader1, length);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.resultSet2.updateCharacterStream(name, reader2, length);
		
		this.replay();

		this.resultSet.updateCharacterStream(name, value, length);
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateClob(int, java.io.Reader)
	 */
	@Test(dataProvider = "int-reader")
	public void updateClob(int index, Reader value) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new StringReader("");
		Reader reader2 = new StringReader("");
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.resultSet1.updateClob(index, reader1);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.resultSet2.updateClob(index, reader2);
		
		this.replay();

		this.resultSet.updateClob(index, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateClob(java.lang.String, java.io.Reader)
	 */
	@Test(dataProvider = "string-reader")
	public void updateClob(String name, Reader value) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new StringReader("");
		Reader reader2 = new StringReader("");
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.resultSet1.updateClob(name, reader1);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.resultSet2.updateClob(name, reader2);
		
		this.replay();

		this.resultSet.updateClob(name, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateClob(int, java.io.Reader, long)
	 */
	@Test(dataProvider = "int-reader-long")
	public void updateClob(int index, Reader value, long length) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new StringReader("");
		Reader reader2 = new StringReader("");
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.resultSet1.updateClob(index, reader1, length);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.resultSet2.updateClob(index, reader2, length);
		
		this.replay();

		this.resultSet.updateClob(index, value, length);
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateClob(java.lang.String, java.io.Reader, long)
	 */
	@Test(dataProvider = "string-reader-long")
	public void updateClob(String name, Reader value, long length) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new StringReader("");
		Reader reader2 = new StringReader("");
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.resultSet1.updateClob(name, reader1, length);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.resultSet2.updateClob(name, reader2, length);
		
		this.replay();

		this.resultSet.updateClob(name, value, length);
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateNCharacterStream(int, java.io.Reader)
	 */
	@Test(dataProvider = "int-reader")
	public void updateNCharacterStream(int index, Reader value) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new StringReader("");
		Reader reader2 = new StringReader("");
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.resultSet1.updateNCharacterStream(index, reader1);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.resultSet2.updateNCharacterStream(index, reader2);
		
		this.replay();

		this.resultSet.updateNCharacterStream(index, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateNCharacterStream(java.lang.String, java.io.Reader)
	 */
	@Test(dataProvider = "string-reader")
	public void updateNCharacterStream(String name, Reader value) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new StringReader("");
		Reader reader2 = new StringReader("");
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.resultSet1.updateNCharacterStream(name, reader1);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.resultSet2.updateNCharacterStream(name, reader2);
		
		this.replay();

		this.resultSet.updateNCharacterStream(name, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateNCharacterStream(int, java.io.Reader, long)
	 */
	@Test(dataProvider = "int-reader-long")
	public void updateNCharacterStream(int index, Reader value, long length) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new StringReader("");
		Reader reader2 = new StringReader("");
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.resultSet1.updateNCharacterStream(index, reader1, length);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.resultSet2.updateNCharacterStream(index, reader2, length);
		
		this.replay();

		this.resultSet.updateNCharacterStream(index, value, length);
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateNCharacterStream(java.lang.String, java.io.Reader, long)
	 */
	@Test(dataProvider = "string-reader-long")
	public void updateNCharacterStream(String name, Reader value, long length) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new StringReader("");
		Reader reader2 = new StringReader("");
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.resultSet1.updateNCharacterStream(name, reader1, length);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.resultSet2.updateNCharacterStream(name, reader2, length);
		
		this.replay();

		this.resultSet.updateNCharacterStream(name, value, length);
		
		this.verify();
	}

	@DataProvider(name = "int-nclob")
	Object[][] intNClobProvider() throws Exception
	{
		Map<Database, NClob> map = new TreeMap<Database, NClob>();
		
		map.put(this.database1, this.nClob1);
		map.put(this.database2, this.nClob2);
		
		NClob clob = ProxyFactory.createProxy(NClob.class, new ClobInvocationHandler(null, this.handler, null, map));
		
		return new Object[][] { new Object[] { 1, new MockClob() }, new Object[] { 1, clob } };
	}
	
	/**
	 * @see java.sql.ResultSet#updateNClob(int, java.sql.NClob)
	 */
	@Test(dataProvider = "int-nclob")
	public void updateNClob(int index, NClob value) throws SQLException
	{
		if (Proxy.isProxyClass(value.getClass()))
		{
			this.resultSet1.updateNClob(index, this.nClob1);
			this.resultSet2.updateNClob(index, this.nClob2);
		}
		else
		{
			this.resultSet1.updateNClob(EasyMock.eq(index), EasyMock.isA(NClob.class));
			this.resultSet2.updateNClob(EasyMock.eq(index), EasyMock.isA(NClob.class));
		}
		
		this.replay();

		this.resultSet.updateNClob(index, value);
		
		this.verify();
	}

	@DataProvider(name = "string-nclob")
	Object[][] stringNClobProvider() throws Exception
	{
		Map<Database, NClob> map = new TreeMap<Database, NClob>();
		
		map.put(this.database1, this.nClob1);
		map.put(this.database2, this.nClob2);
		
		this.replay();
		
		NClob clob = ProxyFactory.createProxy(NClob.class, new ClobInvocationHandler(null, this.handler, null, map));
		
		this.verify();
		this.reset();
		
		return new Object[][] { new Object[] { "", new MockClob() }, new Object[] { "", clob } };
	}
	
	/**
	 * @see java.sql.ResultSet#updateNClob(java.lang.String, java.sql.NClob)
	 */
	@Test(dataProvider = "string-nclob")
	public void updateNClob(String name, NClob value) throws SQLException
	{
		if (Proxy.isProxyClass(value.getClass()))
		{
			this.resultSet1.updateNClob(name, this.nClob1);
			this.resultSet2.updateNClob(name, this.nClob2);
		}
		else
		{
			this.resultSet1.updateNClob(EasyMock.eq(name), EasyMock.isA(NClob.class));
			this.resultSet2.updateNClob(EasyMock.eq(name), EasyMock.isA(NClob.class));
		}
		
		this.replay();

		this.resultSet.updateNClob(name, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateNClob(int, java.io.Reader)
	 */
	@Test(dataProvider = "int-reader")
	public void updateNClob(int index, Reader value) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new StringReader("");
		Reader reader2 = new StringReader("");
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.resultSet1.updateNClob(index, reader1);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.resultSet2.updateNClob(index, reader2);
		
		this.replay();

		this.resultSet.updateNClob(index, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateNClob(java.lang.String, java.io.Reader)
	 */
	@Test(dataProvider = "string-reader")
	public void updateNClob(String name, Reader value) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new StringReader("");
		Reader reader2 = new StringReader("");
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.resultSet1.updateNClob(name, reader1);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.resultSet2.updateNClob(name, reader2);
		
		this.replay();

		this.resultSet.updateNClob(name, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateNClob(int, java.io.Reader, long)
	 */
	@Test(dataProvider = "int-reader-long")
	public void updateNClob(int index, Reader value, long length) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new StringReader("");
		Reader reader2 = new StringReader("");
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.resultSet1.updateNClob(index, reader1, length);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.resultSet2.updateNClob(index, reader2, length);
		
		this.replay();

		this.resultSet.updateNClob(index, value, length);
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateNClob(java.lang.String, java.io.Reader, long)
	 */
	@Test(dataProvider = "string-reader-long")
	public void updateNClob(String name, Reader value, long length) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new StringReader("");
		Reader reader2 = new StringReader("");
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.resultSet1.updateNClob(name, reader1, length);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.resultSet2.updateNClob(name, reader2, length);
		
		this.replay();

		this.resultSet.updateNClob(name, value, length);
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateNString(int, java.lang.String)
	 */
	@Test(dataProvider = "int-string")
	public void updateNString(int index, String value) throws SQLException
	{
		this.resultSet1.updateNString(index, value);
		this.resultSet2.updateNString(index, value);
		
		this.replay();
		
		this.resultSet.updateNString(index, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateNString(java.lang.String, java.lang.String)
	 */
	@Test(dataProvider = "string-string")
	public void updateNString(String name, String value) throws SQLException
	{
		this.resultSet1.updateNString(name, value);
		this.resultSet2.updateNString(name, value);
		
		this.replay();
		
		this.resultSet.updateNString(name, value);
		
		this.verify();
	}

	@DataProvider(name = "int-rowid")
	Object[][] intRowIdProvider()
	{
		return new Object[][] { new Object[] { 1, EasyMock.createMock(RowId.class) } };
	}
	
	/**
	 * @see java.sql.ResultSet#updateRowId(int, java.sql.RowId)
	 */
	@Test(dataProvider = "int-rowid")
	public void updateRowId(int index, RowId value) throws SQLException
	{
		this.resultSet1.updateRowId(index, value);
		this.resultSet2.updateRowId(index, value);
		
		this.replay();
		
		this.resultSet.updateRowId(index, value);
		
		this.verify();
	}

	@DataProvider(name = "string-rowid")
	Object[][] stringRowIdProvider()
	{
		return new Object[][] { new Object[] { "", EasyMock.createMock(RowId.class) } };
	}
	
	/**
	 * @see java.sql.ResultSet#updateRowId(java.lang.String, java.sql.RowId)
	 */
	@Test(dataProvider = "string-rowid")
	public void updateRowId(String name, RowId value) throws SQLException
	{
		this.resultSet1.updateRowId(name, value);
		this.resultSet2.updateRowId(name, value);
		
		this.replay();
		
		this.resultSet.updateRowId(name, value);
		
		this.verify();
	}

	@DataProvider(name = "int-xml")
	Object[][] intSQLXMLProvider()
	{
		return new Object[][] { new Object[] { 1, EasyMock.createMock(SQLXML.class) } };
	}
	
	/**
	 * @see java.sql.ResultSet#updateSQLXML(int, java.sql.SQLXML)
	 */
	@Test(dataProvider = "int-xml")
	public void updateSQLXML(int index, SQLXML value) throws SQLException
	{
		this.resultSet1.updateSQLXML(index, value);
		this.resultSet2.updateSQLXML(index, value);
		
		this.replay();
		
		this.resultSet.updateSQLXML(index, value);
		
		this.verify();
	}

	@DataProvider(name = "string-xml")
	Object[][] stringSQLXMLProvider()
	{
		return new Object[][] { new Object[] { "", EasyMock.createMock(SQLXML.class) } };
	}
	
	/**
	 * @see java.sql.ResultSet#updateSQLXML(java.lang.String, java.sql.SQLXML)
	 */
	@Test(dataProvider = "string-xml")
	public void updateSQLXML(String name, SQLXML value) throws SQLException
	{
		this.resultSet1.updateSQLXML(name, value);
		this.resultSet2.updateSQLXML(name, value);
		
		this.replay();
		
		this.resultSet.updateSQLXML(name, value);
		
		this.verify();
	}

	@DataProvider(name = "class")
	Object[][] classProvider()
	{
		return new Object[][] { new Object[] { Object.class } };
	}

	/**
	 * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
	 */
	@Test(dataProvider = "class")
	public boolean isWrapperFor(Class<?> targetClass) throws SQLException
	{
		EasyMock.expect(this.resultSet1.isWrapperFor(targetClass)).andReturn(true);

		this.replay();
		
		boolean result = this.resultSet.isWrapperFor(targetClass);
		
		return result;
	}

	/**
	 * @see java.sql.Wrapper#unwrap(java.lang.Class)
	 */
	@Test(dataProvider = "class")
	public <T> T unwrap(Class<T> targetClass) throws SQLException
	{
		try
		{
			EasyMock.expect(this.resultSet1.unwrap(targetClass)).andReturn(targetClass.newInstance());
	
			this.replay();
			
			T result = this.resultSet.unwrap(targetClass);
			
			return result;
		}
		catch (InstantiationException e)
		{
			assert false : e;
			return null;
		}
		catch (IllegalAccessException e)
		{
			assert false : e;
			return null;
		}
	}	
}
