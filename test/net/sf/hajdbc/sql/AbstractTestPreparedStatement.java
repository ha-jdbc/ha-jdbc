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
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
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
 * Unit test for {@link PreparedStatement}
 * @author  Paul Ferraro
 */
@Test
@SuppressWarnings({ "unchecked", "nls" })
public abstract class AbstractTestPreparedStatement<S extends PreparedStatement> extends AbstractTestStatement<S> implements java.sql.PreparedStatement
{
	protected String sql = "sql";
	
	protected Blob blob1 = EasyMock.createMock(Blob.class);
	protected Blob blob2 = EasyMock.createMock(Blob.class);
	protected Clob clob1 = EasyMock.createMock(Clob.class);
	protected Clob clob2 = EasyMock.createMock(Clob.class);
	protected NClob nClob1 = EasyMock.createMock(NClob.class);
	protected NClob nClob2 = EasyMock.createMock(NClob.class);
	
	/**
	 * @see net.sf.hajdbc.sql.AbstractTestStatement#recordConstructor()
	 */
	@Override
	protected void recordConstructor() throws SQLException
	{
		this.parent.addChild(EasyMock.isA(PreparedStatementInvocationHandler.class));
		
		this.expectIdentifiers(this.sql, null, null);
		this.expectSelectForUpdateCheck(this.sql, false);
	}

	/**
	 * @see java.sql.PreparedStatement#addBatch()
	 */
	@Test
	public void addBatch() throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.addBatch();
		this.statement2.addBatch();
		
		this.replay();
		
		this.statement.addBatch();
		
		this.verify();
	}

	/**
	 * @see java.sql.PreparedStatement#clearParameters()
	 */
	@Test
	public void clearParameters() throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		this.statement1.addBatch();
		this.statement2.addBatch();
		
		this.replay();
		
		this.statement.addBatch();
	}

	public void testExecute() throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.execute()).andReturn(true);
		EasyMock.expect(this.statement2.execute()).andReturn(true);
		
		this.replay();
		
		boolean result = this.execute();
		
		this.verify();
		
		assert result;
	}
	
	/**
	 * @see java.sql.PreparedStatement#execute()
	 */
	@Override
	public boolean execute() throws SQLException
	{
		return this.statement.execute();
	}

	public void testExecuteQuery() throws SQLException
	{
		ResultSet resultSet = EasyMock.createMock(ResultSet.class);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true).times(2);
		
		// Read-only
		EasyMock.expect(this.statement1.getResultSetConcurrency()).andReturn(ResultSet.CONCUR_READ_ONLY);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database2);

		this.balancer.beforeInvocation(this.database2);
		
		EasyMock.expect(this.statement2.executeQuery()).andReturn(resultSet);

		this.balancer.afterInvocation(this.database2);
		
		this.replay();
		
		ResultSet results = this.executeQuery();
		
		this.verify();
		
		assert results == resultSet;
		
		this.reset();
		
		ResultSet resultSet1 = EasyMock.createMock(ResultSet.class);
		ResultSet resultSet2 = EasyMock.createMock(ResultSet.class);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true).times(2);
		
		// Updatable
		EasyMock.expect(this.statement1.getResultSetConcurrency()).andReturn(ResultSet.CONCUR_UPDATABLE);

		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);

		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.executeQuery()).andReturn(resultSet1);
		EasyMock.expect(this.statement2.executeQuery()).andReturn(resultSet2);

		this.replay();
		
		results = this.executeQuery();
		
		this.verify();
		
		assert Proxy.isProxyClass(results.getClass());
		assert SQLProxy.class.cast(Proxy.getInvocationHandler(results)).getObject(this.database1) == resultSet1;
		assert SQLProxy.class.cast(Proxy.getInvocationHandler(results)).getObject(this.database2) == resultSet2;
	}
	
	/**
	 * @see java.sql.PreparedStatement#executeQuery()
	 */
	@Override
	public ResultSet executeQuery() throws SQLException
	{
		return this.statement.executeQuery();
	}

	public void testExecuteUpdate() throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);

		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.executeUpdate()).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate()).andReturn(1);
		
		this.replay();
		
		int result = this.executeUpdate();
		
		this.verify();
		
		assert result == 1;
	}
	
	/**
	 * @see java.sql.PreparedStatement#executeUpdate()
	 */
	@Override
	public int executeUpdate() throws SQLException
	{
		return this.statement.executeUpdate();
	}

	public void testGetMetaData() throws SQLException
	{
		ResultSetMetaData metaData = EasyMock.createMock(ResultSetMetaData.class);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
	
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database2);
		
		this.balancer.beforeInvocation(this.database2);
		
		EasyMock.expect(this.statement2.getMetaData()).andReturn(metaData);
		
		this.balancer.afterInvocation(this.database2);
		
		this.replay();
		
		ResultSetMetaData result = this.getMetaData();
		
		this.verify();
		
		assert result == metaData;
	}
	
	/**
	 * @see java.sql.PreparedStatement#getMetaData()
	 */
	@Override
	public ResultSetMetaData getMetaData() throws SQLException
	{
		return this.statement.getMetaData();
	}

	public void testGetParameterMetaData() throws SQLException
	{
		ParameterMetaData metaData = EasyMock.createMock(ParameterMetaData.class);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database2);
		
		this.balancer.beforeInvocation(this.database2);
		
		EasyMock.expect(this.statement2.getParameterMetaData()).andReturn(metaData);
		
		this.balancer.afterInvocation(this.database2);
		
		this.replay();
		
		ParameterMetaData result = this.getParameterMetaData();
		
		this.verify();
		
		assert result == metaData;
	}
	
	/**
	 * @see java.sql.PreparedStatement#getParameterMetaData()
	 */
	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException
	{
		return this.statement.getParameterMetaData();
	}

	@DataProvider(name = "int-array")
	Object[][] intArrayProvider()
	{
		return new Object[][] { new Object[] { 1, EasyMock.createMock(Array.class) } };
	}
	
	/**
	 * @see java.sql.PreparedStatement#setArray(int, java.sql.Array)
	 */
	@Test(dataProvider = "int-array")
	public void setArray(int index, Array array) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setArray(index, array);
		this.statement2.setArray(index, array);
		
		this.replay();

		this.statement.setArray(index, array);
		
		this.verify();
	}

	@DataProvider(name = "int-inputStream-int")
	Object[][] intInputStreamIntProvider()
	{
		return new Object[][] { new Object[] { 1, new ByteArrayInputStream(new byte[0]), 0 } };
	}

	/**
	 * @see java.sql.PreparedStatement#setAsciiStream(int, java.io.InputStream, int)
	 */
	@Test(dataProvider = "int-inputStream-int")
	public void setAsciiStream(int index, InputStream inputStream, int length) throws SQLException
	{
		File file = new File("");
		InputStream input1 = new ByteArrayInputStream(new byte[0]);
		InputStream input2 = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(inputStream)).andReturn(file);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.statement1.setAsciiStream(index, input1, length);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.statement2.setAsciiStream(index, input2, length);
		
		this.replay();

		this.statement.setAsciiStream(index, inputStream, length);
		
		this.verify();
	}

	@DataProvider(name = "int-bigDecimal")
	Object[][] intBigDecimalProvider()
	{
		return new Object[][] { new Object[] { 1, new BigDecimal(10) } };
	}
	
	/**
	 * @see java.sql.PreparedStatement#setBigDecimal(int, java.math.BigDecimal)
	 */
	@Test(dataProvider = "int-bigDecimal")
	public void setBigDecimal(int index, BigDecimal value) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setBigDecimal(index, value);
		this.statement2.setBigDecimal(index, value);
		
		this.replay();

		this.statement.setBigDecimal(index, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.PreparedStatement#setBinaryStream(int, java.io.InputStream, int)
	 */
	@Test(dataProvider = "int-inputStream-int")
	public void setBinaryStream(int index, InputStream inputStream, int length) throws SQLException
	{
		File file = new File("");
		InputStream input1 = new ByteArrayInputStream(new byte[0]);
		InputStream input2 = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(inputStream)).andReturn(file);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.statement1.setBinaryStream(index, input1, length);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.statement2.setBinaryStream(index, input2, length);
		
		this.replay();

		this.statement.setBinaryStream(index, inputStream, length);
		
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
	 * @see java.sql.PreparedStatement#setBlob(int, java.sql.Blob)
	 */
	@Test(dataProvider = "int-blob")
	public void setBlob(int index, Blob value) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		if (Proxy.isProxyClass(value.getClass()))
		{
			this.statement1.setBlob(index, this.blob1);
			this.statement2.setBlob(index, this.blob2);
		}
		else
		{
			this.statement1.setBlob(EasyMock.eq(index), EasyMock.isA(SerialBlob.class));
			this.statement2.setBlob(EasyMock.eq(index), EasyMock.isA(SerialBlob.class));
		}
		
		this.replay();

		this.statement.setBlob(index, value);
		
		this.verify();
	}

	@DataProvider(name = "int-boolean")
	Object[][] intBooleanProvider()
	{
		return new Object[][] { new Object[] { 1, true } };
	}

	/**
	 * @see java.sql.PreparedStatement#setBoolean(int, boolean)
	 */
	@Test(dataProvider = "int-boolean")
	public void setBoolean(int index, boolean value) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setBoolean(index, value);
		this.statement2.setBoolean(index, value);
		
		this.replay();

		this.statement.setBoolean(index, value);
		
		this.verify();
	}

	@DataProvider(name = "int-byte")
	Object[][] intByteProvider()
	{
		return new Object[][] { new Object[] { 1, Integer.valueOf(1).byteValue() } };
	}

	/**
	 * @see java.sql.PreparedStatement#setByte(int, byte)
	 */
	@Test(dataProvider = "int-byte")
	public void setByte(int index, byte value) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setByte(index, value);
		this.statement2.setByte(index, value);
		
		this.replay();

		this.statement.setByte(index, value);
		
		this.verify();
	}

	@DataProvider(name = "int-bytes")
	Object[][] intBytesProvider()
	{
		return new Object[][] { new Object[] { 1, new byte[0] } };
	}

	/**
	 * @see java.sql.PreparedStatement#setBytes(int, byte[])
	 */
	@Test(dataProvider = "int-bytes")
	public void setBytes(int index, byte[] value) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setBytes(index, value);
		this.statement2.setBytes(index, value);
		
		this.replay();

		this.statement.setBytes(index, value);
		
		this.verify();
	}

	@DataProvider(name = "int-reader-int")
	Object[][] intReaderIntProvider()
	{
		return new Object[][] { new Object[] { 1, new CharArrayReader(new char[0]), 0 } };
	}

	/**
	 * @see java.sql.PreparedStatement#setCharacterStream(int, java.io.Reader, int)
	 */
	@Test(dataProvider = "int-reader-int")
	public void setCharacterStream(int index, Reader reader, int length) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new CharArrayReader(new char[0]);
		Reader reader2 = new CharArrayReader(new char[0]);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(reader)).andReturn(file);

		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.statement1.setCharacterStream(index, reader1, length);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.statement2.setCharacterStream(index, reader2, length);
		
		this.replay();

		this.statement.setCharacterStream(index, reader, length);
		
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
	 * @see java.sql.PreparedStatement#setClob(int, java.sql.Clob)
	 */
	@Test(dataProvider = "int-clob")
	public void setClob(int index, Clob value) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		if (Proxy.isProxyClass(value.getClass()))
		{
			this.statement1.setClob(index, this.clob1);
			this.statement2.setClob(index, this.clob2);
		}
		else
		{
			this.statement1.setClob(EasyMock.eq(index), EasyMock.isA(SerialClob.class));
			this.statement2.setClob(EasyMock.eq(index), EasyMock.isA(SerialClob.class));
		}
		
		this.replay();

		this.statement.setClob(index, value);
		
		this.verify();
	}

	@DataProvider(name = "int-date")
	Object[][] intDateProvider()
	{
		return new Object[][] { new Object[] { 1, new Date(System.currentTimeMillis()) } };
	}

	/**
	 * @see java.sql.PreparedStatement#setDate(int, java.sql.Date)
	 */
	@Test(dataProvider = "int-date")
	public void setDate(int index, Date date) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setDate(index, date);
		this.statement2.setDate(index, date);
		
		this.replay();

		this.statement.setDate(index, date);
		
		this.verify();
	}

	@DataProvider(name = "int-date-calendar")
	Object[][] intDateCalendarProvider()
	{
		return new Object[][] { new Object[] { 1, new Date(System.currentTimeMillis()), Calendar.getInstance() } };
	}

	/**
	 * @see java.sql.PreparedStatement#setDate(int, java.sql.Date, java.util.Calendar)
	 */
	@Test(dataProvider = "int-date-calendar")
	public void setDate(int index, Date date, Calendar calendar) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setDate(index, date, calendar);
		this.statement2.setDate(index, date, calendar);
		
		this.replay();

		this.statement.setDate(index, date, calendar);
		
		this.verify();
	}

	@DataProvider(name = "int-double")
	Object[][] intDoubleProvider()
	{
		return new Object[][] { new Object[] { 1, 1.0 } };
	}

	/**
	 * @see java.sql.PreparedStatement#setDouble(int, double)
	 */
	@Test(dataProvider = "int-double")
	public void setDouble(int index, double value) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setDouble(index, value);
		this.statement2.setDouble(index, value);
		
		this.replay();

		this.statement.setDouble(index, value);
		
		this.verify();
	}

	@DataProvider(name = "int-float")
	Object[][] intFloatProvider()
	{
		return new Object[][] { new Object[] { 1, 1.0f } };
	}

	/**
	 * @see java.sql.PreparedStatement#setFloat(int, float)
	 */
	@Test(dataProvider = "int-float")
	public void setFloat(int index, float value) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setFloat(index, value);
		this.statement2.setFloat(index, value);
		
		this.replay();

		this.statement.setFloat(index, value);
		
		this.verify();
	}

	@DataProvider(name = "int-int")
	Object[][] intIntProvider()
	{
		return new Object[][] { new Object[] { 1, 1 } };
	}

	/**
	 * @see java.sql.PreparedStatement#setInt(int, int)
	 */
	@Test(dataProvider = "int-int")
	public void setInt(int index, int value) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setInt(index, value);
		this.statement2.setInt(index, value);
		
		this.replay();

		this.statement.setInt(index, value);
		
		this.verify();
	}

	@DataProvider(name = "int-long")
	Object[][] intLongProvider()
	{
		return new Object[][] { new Object[] { 1, 1L } };
	}

	/**
	 * @see java.sql.PreparedStatement#setLong(int, long)
	 */
	@Test(dataProvider = "int-long")
	public void setLong(int index, long value) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setLong(index, value);
		this.statement2.setLong(index, value);
		
		this.replay();

		this.statement.setLong(index, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.PreparedStatement#setNull(int, int)
	 */
	@Test(dataProvider = "int-int")
	public void setNull(int index, int sqlType) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setNull(index, sqlType);
		this.statement2.setNull(index, sqlType);
		
		this.replay();

		this.statement.setNull(index, sqlType);
		
		this.verify();
	}

	@DataProvider(name = "int-int-string")
	Object[][] intIntStringProvider()
	{
		return new Object[][] { new Object[] { 1, 1, "" } };
	}

	/**
	 * @see java.sql.PreparedStatement#setNull(int, int, java.lang.String)
	 */
	@Test(dataProvider = "int-int-string")
	public void setNull(int index, int sqlType, String typeName) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setNull(index, sqlType, typeName);
		this.statement2.setNull(index, sqlType, typeName);
		
		this.replay();

		this.statement.setNull(index, sqlType, typeName);
		
		this.verify();
	}

	@DataProvider(name = "int-object")
	Object[][] intObjectProvider()
	{
		return new Object[][] { new Object[] { 1, new Object() } };
	}

	/**
	 * @see java.sql.PreparedStatement#setObject(int, java.lang.Object)
	 */
	@Test(dataProvider = "int-object")
	public void setObject(int index, Object value) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setObject(index, value);
		this.statement2.setObject(index, value);
		
		this.replay();

		this.statement.setObject(index, value);
		
		this.verify();
	}

	@DataProvider(name = "int-object-int")
	Object[][] intObjectIntProvider()
	{
		return new Object[][] { new Object[] { 1, new Object(), 1 } };
	}

	/**
	 * @see java.sql.PreparedStatement#setObject(int, java.lang.Object, int)
	 */
	@Test(dataProvider = "int-object-int")
	public void setObject(int index, Object value, int targetSqlType) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setObject(index, value, targetSqlType);
		this.statement2.setObject(index, value, targetSqlType);
		
		this.replay();

		this.statement.setObject(index, value, targetSqlType);
		
		this.verify();
	}

	@DataProvider(name = "int-object-int-int")
	Object[][] intObjectIntIntProvider()
	{
		return new Object[][] { new Object[] { 1, new Object(), 1, 1 } };
	}

	/**
	 * @see java.sql.PreparedStatement#setObject(int, java.lang.Object, int, int)
	 */
	@Test(dataProvider = "int-object-int-int")
	public void setObject(int index, Object value, int targetSqlType, int scale) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setObject(index, value, targetSqlType, scale);
		this.statement2.setObject(index, value, targetSqlType, scale);
		
		this.replay();

		this.statement.setObject(index, value, targetSqlType, scale);
		
		this.verify();
	}

	@DataProvider(name = "int-ref")
	Object[][] intRefProvider()
	{
		return new Object[][] { new Object[] { 1, EasyMock.createMock(Ref.class) } };
	}

	/**
	 * @see java.sql.PreparedStatement#setRef(int, java.sql.Ref)
	 */
	@Test(dataProvider = "int-ref")
	public void setRef(int index, Ref value) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setRef(index, value);
		this.statement2.setRef(index, value);
		
		this.replay();

		this.statement.setRef(index, value);
		
		this.verify();
	}

	@DataProvider(name = "int-short")
	Object[][] intShortProvider()
	{
		return new Object[][] { new Object[] { 1, Integer.valueOf(1).shortValue() } };
	}

	/**
	 * @see java.sql.PreparedStatement#setShort(int, short)
	 */
	@Test(dataProvider = "int-short")
	public void setShort(int index, short value) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setShort(index, value);
		this.statement2.setShort(index, value);
		
		this.replay();

		this.statement.setShort(index, value);
		
		this.verify();
	}

	@DataProvider(name = "int-string")
	Object[][] intStringProvider()
	{
		return new Object[][] { new Object[] { 1, "" } };
	}

	/**
	 * @see java.sql.PreparedStatement#setString(int, java.lang.String)
	 */
	@Test(dataProvider = "int-string")
	public void setString(int index, String value) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setString(index, value);
		this.statement2.setString(index, value);
		
		this.replay();

		this.statement.setString(index, value);
		
		this.verify();
	}

	@DataProvider(name = "int-time")
	Object[][] intTimeProvider()
	{
		return new Object[][] { new Object[] { 1, new Time(System.currentTimeMillis()) } };
	}

	/**
	 * @see java.sql.PreparedStatement#setTime(int, java.sql.Time)
	 */
	@Test(dataProvider = "int-time")
	public void setTime(int index, Time value) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setTime(index, value);
		this.statement2.setTime(index, value);
		
		this.replay();

		this.statement.setTime(index, value);
		
		this.verify();
	}

	@DataProvider(name = "int-time-calendar")
	Object[][] intTimeCalendarProvider()
	{
		return new Object[][] { new Object[] { 1, new Time(System.currentTimeMillis()), Calendar.getInstance() } };
	}

	/**
	 * @see java.sql.PreparedStatement#setTime(int, java.sql.Time, java.util.Calendar)
	 */
	@Test(dataProvider = "int-time-calendar")
	public void setTime(int index, Time value, Calendar calendar) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setTime(index, value, calendar);
		this.statement2.setTime(index, value, calendar);
		
		this.replay();

		this.statement.setTime(index, value, calendar);
		
		this.verify();
	}

	@DataProvider(name = "int-timestamp")
	Object[][] intTimestampProvider()
	{
		return new Object[][] { new Object[] { 1, new Timestamp(System.currentTimeMillis()) } };
	}

	/**
	 * @see java.sql.PreparedStatement#setTimestamp(int, java.sql.Timestamp)
	 */
	@Test(dataProvider = "int-timestamp")
	public void setTimestamp(int index, Timestamp value) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setTimestamp(index, value);
		this.statement2.setTimestamp(index, value);
		
		this.replay();

		this.statement.setTimestamp(index, value);
		
		this.verify();
	}

	@DataProvider(name = "int-timestamp-calendar")
	Object[][] intTimestampCalendarProvider()
	{
		return new Object[][] { new Object[] { 1, new Timestamp(System.currentTimeMillis()), Calendar.getInstance() } };
	}

	/**
	 * @see java.sql.PreparedStatement#setTimestamp(int, java.sql.Timestamp, java.util.Calendar)
	 */
	@Test(dataProvider = "int-timestamp-calendar")
	public void setTimestamp(int index, Timestamp value, Calendar calendar) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setTimestamp(index, value, calendar);
		this.statement2.setTimestamp(index, value, calendar);
		
		this.replay();

		this.statement.setTimestamp(index, value, calendar);
		
		this.verify();
	}

	/**
	 * @see java.sql.PreparedStatement#setUnicodeStream(int, java.io.InputStream, int)
	 */
	@SuppressWarnings("deprecation")
	@Test(dataProvider = "int-inputStream-int")
	@Deprecated
	public void setUnicodeStream(int index, InputStream inputStream, int length) throws SQLException
	{
		File file = new File("");
		InputStream input1 = new ByteArrayInputStream(new byte[0]);
		InputStream input2 = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(inputStream)).andReturn(file);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.statement1.setUnicodeStream(index, input1, length);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.statement2.setUnicodeStream(index, input2, length);
		
		this.replay();

		this.statement.setUnicodeStream(index, inputStream, length);
		
		this.verify();
	}

	@DataProvider(name = "int-url")
	Object[][] intURLProvider() throws MalformedURLException
	{
		return new Object[][] { new Object[] { 1, new URL("http://ha-jdbc.sf.net") } };
	}

	/**
	 * @see java.sql.PreparedStatement#setURL(int, java.net.URL)
	 */
	@Test(dataProvider = "int-url")
	public void setURL(int index, URL value) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setURL(index, value);
		this.statement2.setURL(index, value);
		
		this.replay();

		this.statement.setURL(index, value);
		
		this.verify();
	}

	@DataProvider(name = "int-inputStream")
	Object[][] intInputStreamProvider()
	{
		return new Object[][] { new Object[] { 1, new ByteArrayInputStream(new byte[0]) } };
	}
	
	/**
	 * @see java.sql.PreparedStatement#setAsciiStream(int, java.io.InputStream)
	 */
	@Test(dataProvider = "int-inputStream")
	public void setAsciiStream(int index, InputStream value) throws SQLException
	{
		File file = new File("");
		InputStream input1 = new ByteArrayInputStream(new byte[0]);
		InputStream input2 = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.statement1.setAsciiStream(index, input1);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.statement2.setAsciiStream(index, input2);
		
		this.replay();

		this.statement.setAsciiStream(index, value);
		
		this.verify();
	}

	@DataProvider(name = "int-inputStream-long")
	Object[][] intInputStreamLongProvider()
	{
		return new Object[][] { new Object[] { 1, new ByteArrayInputStream(new byte[0]), 1L } };
	}
	
	/**
	 * @see java.sql.PreparedStatement#setAsciiStream(int, java.io.InputStream, long)
	 */
	@Test(dataProvider = "int-inputStream-long")
	public void setAsciiStream(int index, InputStream value, long length) throws SQLException
	{
		File file = new File("");
		InputStream input1 = new ByteArrayInputStream(new byte[0]);
		InputStream input2 = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.statement1.setAsciiStream(index, input1, length);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.statement2.setAsciiStream(index, input2, length);
		
		this.replay();

		this.statement.setAsciiStream(index, value, length);
		
		this.verify();
	}

	/**
	 * @see java.sql.PreparedStatement#setBinaryStream(int, java.io.InputStream)
	 */
	@Test(dataProvider = "int-inputStream")
	public void setBinaryStream(int index, InputStream value) throws SQLException
	{
		File file = new File("");
		InputStream input1 = new ByteArrayInputStream(new byte[0]);
		InputStream input2 = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.statement1.setBinaryStream(index, input1);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.statement2.setBinaryStream(index, input2);
		
		this.replay();

		this.statement.setBinaryStream(index, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.PreparedStatement#setBinaryStream(int, java.io.InputStream, long)
	 */
	@Test(dataProvider = "int-inputStream-long")
	public void setBinaryStream(int index, InputStream value, long length) throws SQLException
	{
		File file = new File("");
		InputStream input1 = new ByteArrayInputStream(new byte[0]);
		InputStream input2 = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.statement1.setBinaryStream(index, input1, length);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.statement2.setBinaryStream(index, input2, length);
		
		this.replay();

		this.statement.setBinaryStream(index, value, length);
		
		this.verify();
	}

	/**
	 * @see java.sql.PreparedStatement#setBlob(int, java.io.InputStream)
	 */
	@Test(dataProvider = "int-inputStream")
	public void setBlob(int index, InputStream value) throws SQLException
	{
		File file = new File("");
		InputStream input1 = new ByteArrayInputStream(new byte[0]);
		InputStream input2 = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.statement1.setBlob(index, input1);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.statement2.setBlob(index, input2);
		
		this.replay();

		this.statement.setBlob(index, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.PreparedStatement#setBlob(int, java.io.InputStream, long)
	 */
	@Test(dataProvider = "int-inputStream-long")
	public void setBlob(int index, InputStream value, long length) throws SQLException
	{
		File file = new File("");
		InputStream input1 = new ByteArrayInputStream(new byte[0]);
		InputStream input2 = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input1);
		
		this.statement1.setBlob(index, input1, length);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input2);
		
		this.statement2.setBlob(index, input2, length);
		
		this.replay();

		this.statement.setBlob(index, value, length);
		
		this.verify();
	}

	/**
	 * @see java.sql.PreparedStatement#setCharacterStream(int, java.io.Reader)
	 */
	@Test(dataProvider = "int-reader")
	public void setCharacterStream(int index, Reader value) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new CharArrayReader(new char[0]);
		Reader reader2 = new CharArrayReader(new char[0]);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.statement1.setCharacterStream(index, reader1);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.statement2.setCharacterStream(index, reader2);
		
		this.replay();

		this.statement.setCharacterStream(index, value);
		
		this.verify();
	}

	@DataProvider(name = "int-reader-long")
	Object[][] intReaderLongProvider()
	{
		return new Object[][] { new Object[] { 1, new StringReader(""), 1L } };
	}

	/**
	 * @see java.sql.PreparedStatement#setCharacterStream(int, java.io.Reader, long)
	 */
	@Test(dataProvider = "int-reader-long")
	public void setCharacterStream(int index, Reader value, long length) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new CharArrayReader(new char[0]);
		Reader reader2 = new CharArrayReader(new char[0]);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.statement1.setCharacterStream(index, reader1, length);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.statement2.setCharacterStream(index, reader2, length);
		
		this.replay();

		this.statement.setCharacterStream(index, value, length);
		
		this.verify();
	}

	@DataProvider(name = "int-reader")
	Object[][] intReaderProvider()
	{
		return new Object[][] { new Object[] { 1, new StringReader("") } };
	}

	/**
	 * @see java.sql.PreparedStatement#setClob(int, java.io.Reader)
	 */
	@Test(dataProvider = "int-reader")
	public void setClob(int index, Reader value) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new CharArrayReader(new char[0]);
		Reader reader2 = new CharArrayReader(new char[0]);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.statement1.setClob(index, reader1);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.statement2.setClob(index, reader2);
		
		this.replay();

		this.statement.setClob(index, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.PreparedStatement#setClob(int, java.io.Reader, long)
	 */
	@Test(dataProvider = "int-reader-long")
	public void setClob(int index, Reader value, long length) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new CharArrayReader(new char[0]);
		Reader reader2 = new CharArrayReader(new char[0]);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.statement1.setClob(index, reader1, length);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.statement2.setClob(index, reader2, length);
		
		this.replay();

		this.statement.setClob(index, value, length);
		
		this.verify();
	}

	/**
	 * @see java.sql.PreparedStatement#setNCharacterStream(int, java.io.Reader)
	 */
	@Test(dataProvider = "int-reader")
	public void setNCharacterStream(int index, Reader value) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new CharArrayReader(new char[0]);
		Reader reader2 = new CharArrayReader(new char[0]);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.statement1.setNCharacterStream(index, reader1);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.statement2.setNCharacterStream(index, reader2);
		
		this.replay();

		this.statement.setNCharacterStream(index, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.PreparedStatement#setNCharacterStream(int, java.io.Reader, long)
	 */
	@Test(dataProvider = "int-reader-long")
	public void setNCharacterStream(int index, Reader value, long length) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new CharArrayReader(new char[0]);
		Reader reader2 = new CharArrayReader(new char[0]);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.statement1.setNCharacterStream(index, reader1, length);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.statement2.setNCharacterStream(index, reader2, length);
		
		this.replay();

		this.statement.setNCharacterStream(index, value, length);
		
		this.verify();
	}

	@DataProvider(name = "int-nclob")
	Object[][] intNClobProvider() throws Exception
	{
		Map<Database, NClob> map = new TreeMap<Database, NClob>();
		
		map.put(this.database1, this.nClob1);
		map.put(this.database2, this.nClob2);
		
		NClob nClob = ProxyFactory.createProxy(NClob.class, new ClobInvocationHandler(null, this.handler, null, map));
		
		return new Object[][] { new Object[] { 1, new MockClob() }, new Object[] { 1, nClob } };
	}

	/**
	 * @see java.sql.PreparedStatement#setNClob(int, java.sql.NClob)
	 */
	@Test(dataProvider = "int-nclob")
	public void setNClob(int index, NClob value) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		if (Proxy.isProxyClass(value.getClass()))
		{
			this.statement1.setNClob(index, this.nClob1);
			this.statement2.setNClob(index, this.nClob2);
		}
		else
		{
			this.statement1.setNClob(EasyMock.eq(index), EasyMock.isA(NClob.class));
			this.statement2.setNClob(EasyMock.eq(index), EasyMock.isA(NClob.class));
		}
		
		this.replay();

		this.statement.setNClob(index, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.PreparedStatement#setNClob(int, java.io.Reader)
	 */
	@Test(dataProvider = "int-reader")
	public void setNClob(int index, Reader value) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new CharArrayReader(new char[0]);
		Reader reader2 = new CharArrayReader(new char[0]);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.statement1.setNClob(index, reader1);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.statement2.setNClob(index, reader2);
		
		this.replay();

		this.statement.setNClob(index, value);
		
		this.verify();
	}

	/**
	 * @see java.sql.PreparedStatement#setNClob(int, java.io.Reader, long)
	 */
	@Test(dataProvider = "int-reader-long")
	public void setNClob(int index, Reader value, long length) throws SQLException
	{
		File file = new File("");
		Reader reader1 = new CharArrayReader(new char[0]);
		Reader reader2 = new CharArrayReader(new char[0]);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);

		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader1);
		
		this.statement1.setNClob(index, reader1, length);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader2);
		
		this.statement2.setNClob(index, reader2, length);
		
		this.replay();

		this.statement.setNClob(index, value, length);
		
		this.verify();
	}

	/**
	 * @see java.sql.PreparedStatement#setNString(int, java.lang.String)
	 */
	@Test(dataProvider = "int-string")
	public void setNString(int index, String value) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setNString(index, value);
		this.statement2.setNString(index, value);
		
		this.replay();

		this.statement.setNString(index, value);
		
		this.verify();
	}

	@DataProvider(name = "int-rowId")
	Object[][] intRowIdProvider()
	{
		return new Object[][] { new Object[] { 1, EasyMock.createMock(RowId.class) } };
	}

	/**
	 * @see java.sql.PreparedStatement#setRowId(int, java.sql.RowId)
	 */
	@Test(dataProvider = "int-rowId")
	public void setRowId(int index, RowId value) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setRowId(index, value);
		this.statement2.setRowId(index, value);
		
		this.replay();

		this.statement.setRowId(index, value);
		
		this.verify();
	}

	@DataProvider(name = "int-xml")
	Object[][] intSQLXMLProvider()
	{
		return new Object[][] { new Object[] { 1, EasyMock.createMock(SQLXML.class) } };
	}
	
	/**
	 * @see java.sql.PreparedStatement#setSQLXML(int, java.sql.SQLXML)
	 */
	@Test(dataProvider = "int-xml")
	public void setSQLXML(int index, SQLXML value) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setSQLXML(index, value);
		this.statement2.setSQLXML(index, value);
		
		this.replay();

		this.statement.setSQLXML(index, value);
		
		this.verify();
	}
}
