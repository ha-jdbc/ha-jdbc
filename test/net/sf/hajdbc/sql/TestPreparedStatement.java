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
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.LockManager;
import net.sf.hajdbc.Operation;

import org.easymock.EasyMock;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test for {@link PreparedStatement}
 * @author  Paul Ferraro
 * @since   1.1
 */
public class TestPreparedStatement extends TestStatement implements java.sql.PreparedStatement
{
	protected String sql = "sql";
	
	/**
	 * @see net.sf.hajdbc.sql.TestStatement#getStatementClass()
	 */
	@Override
	protected Class<? extends java.sql.Statement> getStatementClass()
	{
		return java.sql.PreparedStatement.class;
	}

	/**
	 * @see net.sf.hajdbc.sql.TestStatement#createStatement(net.sf.hajdbc.sql.Connection)
	 */
	@Override
	protected Statement createStatement(Connection connection) throws SQLException
	{
		Operation<java.sql.Connection, java.sql.PreparedStatement> operation = new Operation<java.sql.Connection, java.sql.PreparedStatement>()
		{
			public java.sql.PreparedStatement execute(Database database, java.sql.Connection connection)
			{
				return TestPreparedStatement.this.getSQLStatement();
			}
		};
		
		return new PreparedStatement<java.sql.PreparedStatement>(connection, operation, this.sql);
	}

	protected java.sql.PreparedStatement getSQLStatement()
	{
		return java.sql.PreparedStatement.class.cast(this.sqlStatement);
	}

	private PreparedStatement getStatement()
	{
		return PreparedStatement.class.cast(this.statement);		
	}
	
	/**
	 * @see java.sql.PreparedStatement#addBatch()
	 */
	@Test
	public void addBatch() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.getSQLStatement().addBatch("test");
		
		this.control.replay();
		
		this.getStatement().addBatch("test");
		
		this.control.verify();
	}

	/**
	 * @see java.sql.PreparedStatement#clearParameters()
	 */
	@Test
	public void clearParameters() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.getSQLStatement().clearParameters();
		
		this.control.replay();
		
		this.getStatement().clearParameters();
		
		this.control.verify();
	}

	/**
	 * @see java.sql.PreparedStatement#execute()
	 */
	@Test
	public boolean execute() throws SQLException
	{
		// No sequence
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.parseSequence(this.sql)).andReturn(null);
		
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.getSQLStatement().execute()).andReturn(true);
		
		this.lock.unlock();
		
		this.control.replay();
		
		boolean result = this.getStatement().execute();
		
		this.control.verify();
		
		assert result;

		this.control.reset();
		
		// Sequence
		String sequence = "sequence";
		
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.parseSequence(this.sql)).andReturn(sequence);
		
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.writeLock(sequence)).andReturn(this.lock);
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.getSQLStatement().execute()).andReturn(true);
		
		this.lock.unlock();
		
		this.control.replay();
		
		result = this.getStatement().execute();
		
		this.control.verify();
		
		assert result;
		
		return result;
	}

	/**
	 * @see java.sql.PreparedStatement#executeQuery()
	 */
	@Test
	public ResultSet executeQuery() throws SQLException
	{
		ResultSet resultSet = EasyMock.createMock(ResultSet.class);
		
		// Read-only result set
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.parseSequence(this.sql)).andReturn(null);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlStatement.getResultSetConcurrency()).andReturn(ResultSet.CONCUR_READ_ONLY);
		
		EasyMock.expect(this.databaseCluster.getDatabaseMetaDataCache()).andReturn(this.metaData);
		EasyMock.expect(this.metaData.getDatabaseProperties(this.connection)).andReturn(this.databaseProperties);
		EasyMock.expect(this.databaseProperties.isSelectForUpdateSupported()).andReturn(true);
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.isSelectForUpdate(this.sql)).andReturn(false);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database);

		this.balancer.beforeOperation(this.database);
		
		EasyMock.expect(this.sqlStatement.executeQuery(this.sql)).andReturn(resultSet);

		this.balancer.afterOperation(this.database);
		
		this.control.replay();
		
		ResultSet results = this.statement.executeQuery(this.sql);
		
		this.control.verify();
		
		assert results == resultSet;
		
		this.control.reset();
		
		// Sequence reference
		String sequence = "sequence";
		
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.parseSequence(this.sql)).andReturn(sequence);
		
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.writeLock(sequence)).andReturn(this.lock);
		
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlStatement.executeQuery(this.sql)).andReturn(resultSet);
		
		this.lock.unlock();
		
		this.control.replay();
		
		results = this.statement.executeQuery(this.sql);
		
		this.control.verify();
		
		assert net.sf.hajdbc.sql.ResultSet.class.isInstance(results) : results.getClass().getName();
		assert net.sf.hajdbc.sql.ResultSet.class.cast(results).getObject(this.database) == resultSet;
		
		this.control.reset();
		
		// Updatable result set
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.parseSequence(this.sql)).andReturn(null);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlStatement.getResultSetConcurrency()).andReturn(ResultSet.CONCUR_UPDATABLE);
		
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlStatement.executeQuery(this.sql)).andReturn(resultSet);
		
		this.lock.unlock();
		
		this.control.replay();
		
		results = this.statement.executeQuery(this.sql);
		
		this.control.verify();
		
		assert net.sf.hajdbc.sql.ResultSet.class.isInstance(results) : results.getClass().getName();
		assert net.sf.hajdbc.sql.ResultSet.class.cast(results).getObject(this.database) == resultSet;
		
		this.control.reset();
		
		// SELECT...FOR UPDATE
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.parseSequence(this.sql)).andReturn(null);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlStatement.getResultSetConcurrency()).andReturn(ResultSet.CONCUR_READ_ONLY);
		
		EasyMock.expect(this.databaseCluster.getDatabaseMetaDataCache()).andReturn(this.metaData);
		EasyMock.expect(this.metaData.getDatabaseProperties(this.connection)).andReturn(this.databaseProperties);
		EasyMock.expect(this.databaseProperties.isSelectForUpdateSupported()).andReturn(true);
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.isSelectForUpdate(this.sql)).andReturn(true);
		
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlStatement.executeQuery(this.sql)).andReturn(resultSet);
		
		this.lock.unlock();
		
		this.control.replay();
		
		results = this.statement.executeQuery(this.sql);
		
		this.control.verify();
		
		assert net.sf.hajdbc.sql.ResultSet.class.isInstance(results) : results.getClass().getName();
		assert net.sf.hajdbc.sql.ResultSet.class.cast(results).getObject(this.database) == resultSet;
		
		return results;
	}

	/**
	 * @see java.sql.PreparedStatement#executeUpdate()
	 */
	@Test
	public int executeUpdate() throws SQLException
	{
		int rows = 10;
		
		// No sequence
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.parseSequence(this.sql)).andReturn(null);
		
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.getSQLStatement().executeUpdate()).andReturn(rows);
		
		this.lock.unlock();
		
		this.control.replay();
		
		int results = this.getStatement().executeUpdate();
		
		this.control.verify();
		
		assert results == rows : results;

		this.control.reset();
		
		// Sequence
		String sequence = "sequence";
		
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.parseSequence(this.sql)).andReturn(sequence);
		
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.writeLock(sequence)).andReturn(this.lock);
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.getSQLStatement().executeUpdate()).andReturn(rows);
		
		this.lock.unlock();
		
		this.control.replay();
		
		results = this.getStatement().executeUpdate();
		
		this.control.verify();
		
		assert results == rows : results;
		
		return results;
	}

	/**
	 * @see java.sql.PreparedStatement#getMetaData()
	 */
	@Test
	public ResultSetMetaData getMetaData() throws SQLException
	{
		ResultSetMetaData metaData = EasyMock.createMock(ResultSetMetaData.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database);
		
		this.balancer.beforeOperation(this.database);
		
		EasyMock.expect(this.getSQLStatement().getMetaData()).andReturn(metaData);
		
		this.balancer.afterOperation(this.database);
		
		this.control.replay();
		
		ResultSetMetaData result = this.getStatement().getMetaData();
		
		this.control.verify();
		
		assert result == metaData;
		
		return result;
	}

	/**
	 * @see java.sql.PreparedStatement#getParameterMetaData()
	 */
	@Test
	public ParameterMetaData getParameterMetaData() throws SQLException
	{
		ParameterMetaData metaData = EasyMock.createMock(ParameterMetaData.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database);
		
		this.balancer.beforeOperation(this.database);
		
		EasyMock.expect(this.getSQLStatement().getParameterMetaData()).andReturn(metaData);
		
		this.balancer.afterOperation(this.database);
		
		this.control.replay();
		
		ParameterMetaData result = this.getStatement().getParameterMetaData();
		
		this.control.verify();
		
		assert result == metaData;
		
		return result;
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.getSQLStatement().setArray(index, array);
		
		this.control.replay();

		this.getStatement().setArray(index, array);
		
		this.control.verify();
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
		InputStream input = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.fileSupport.createFile(inputStream)).andReturn(file);
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input);
		
		this.getSQLStatement().setAsciiStream(index, input, length);
		
		this.control.replay();

		this.getStatement().setAsciiStream(index, inputStream, length);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		this.getSQLStatement().setBigDecimal(index, value);
		
		this.control.replay();

		this.getStatement().setBigDecimal(index, value);
		
		this.control.verify();
	}

	/**
	 * @see java.sql.PreparedStatement#setBinaryStream(int, java.io.InputStream, int)
	 */
	@Test(dataProvider = "int-inputStream-int")
	public void setBinaryStream(int index, InputStream inputStream, int length) throws SQLException
	{
		File file = new File("");
		InputStream input = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.fileSupport.createFile(inputStream)).andReturn(file);
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input);
		
		this.getSQLStatement().setBinaryStream(index, input, length);
		
		this.control.replay();

		this.getStatement().setBinaryStream(index, inputStream, length);
		
		this.control.verify();
	}

	@DataProvider(name = "int-blob")
	Object[][] intBlobProvider()
	{
		return new Object[][] { new Object[] { 1, EasyMock.createMock(Blob.class) } };
	}

	/**
	 * @see java.sql.PreparedStatement#setBlob(int, java.sql.Blob)
	 */
	@Test(dataProvider = "int-blob")
	public void setBlob(int index, Blob value) throws SQLException
	{
		File file = new File("");
		Blob blob = EasyMock.createMock(Blob.class);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		EasyMock.expect(this.fileSupport.getBlob(file)).andReturn(blob);
		
		this.getSQLStatement().setBlob(index, blob);
		
		this.control.replay();

		this.getStatement().setBlob(index, value);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		this.getSQLStatement().setBoolean(index, value);
		
		this.control.replay();

		this.getStatement().setBoolean(index, value);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		this.getSQLStatement().setByte(index, value);
		
		this.control.replay();

		this.getStatement().setByte(index, value);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		this.getSQLStatement().setBytes(index, value);
		
		this.control.replay();

		this.getStatement().setBytes(index, value);
		
		this.control.verify();
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
		Reader read = new CharArrayReader(new char[0]);
		
		EasyMock.expect(this.fileSupport.createFile(reader)).andReturn(file);
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(read);
		
		this.getSQLStatement().setCharacterStream(index, read, length);
		
		this.control.replay();

		this.getStatement().setCharacterStream(index, reader, length);
		
		this.control.verify();
	}

	@DataProvider(name = "int-clob")
	Object[][] intClobProvider()
	{
		return new Object[][] { new Object[] { 1, EasyMock.createMock(Clob.class) } };
	}

	/**
	 * @see java.sql.PreparedStatement#setClob(int, java.sql.Clob)
	 */
	@Test(dataProvider = "int-clob")
	public void setClob(int index, Clob value) throws SQLException
	{
		File file = new File("");
		Clob clob = EasyMock.createMock(Clob.class);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		EasyMock.expect(this.fileSupport.getClob(file)).andReturn(clob);
		
		this.getSQLStatement().setClob(index, clob);
		
		this.control.replay();

		this.getStatement().setClob(index, value);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		this.getSQLStatement().setDate(index, date);
		
		this.control.replay();

		this.getStatement().setDate(index, date);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		this.getSQLStatement().setDate(index, date, calendar);
		
		this.control.replay();

		this.getStatement().setDate(index, date, calendar);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		this.getSQLStatement().setDouble(index, value);
		
		this.control.replay();

		this.getStatement().setDouble(index, value);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		this.getSQLStatement().setFloat(index, value);
		
		this.control.replay();

		this.getStatement().setFloat(index, value);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		this.getSQLStatement().setInt(index, value);
		
		this.control.replay();

		this.getStatement().setInt(index, value);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		this.getSQLStatement().setLong(index, value);
		
		this.control.replay();

		this.getStatement().setLong(index, value);
		
		this.control.verify();
	}

	/**
	 * @see java.sql.PreparedStatement#setNull(int, int)
	 */
	@Test(dataProvider = "int-int")
	public void setNull(int index, int sqlType) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		this.getSQLStatement().setNull(index, sqlType);
		
		this.control.replay();

		this.getStatement().setNull(index, sqlType);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		this.getSQLStatement().setNull(index, sqlType, typeName);
		
		this.control.replay();

		this.getStatement().setNull(index, sqlType, typeName);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		this.getSQLStatement().setObject(index, value);
		
		this.control.replay();

		this.getStatement().setObject(index, value);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		this.getSQLStatement().setObject(index, value, targetSqlType);
		
		this.control.replay();

		this.getStatement().setObject(index, value, targetSqlType);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		this.getSQLStatement().setObject(index, value, targetSqlType, scale);
		
		this.control.replay();

		this.getStatement().setObject(index, value, targetSqlType, scale);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		this.getSQLStatement().setRef(index, value);
		
		this.control.replay();

		this.getStatement().setRef(index, value);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		this.getSQLStatement().setShort(index, value);
		
		this.control.replay();

		this.getStatement().setShort(index, value);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		this.getSQLStatement().setString(index, value);
		
		this.control.replay();

		this.getStatement().setString(index, value);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		this.getSQLStatement().setTime(index, value);
		
		this.control.replay();

		this.getStatement().setTime(index, value);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		this.getSQLStatement().setTime(index, value, calendar);
		
		this.control.replay();

		this.getStatement().setTime(index, value, calendar);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		this.getSQLStatement().setTimestamp(index, value);
		
		this.control.replay();

		this.getStatement().setTimestamp(index, value);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		this.getSQLStatement().setTimestamp(index, value, calendar);
		
		this.control.replay();

		this.getStatement().setTimestamp(index, value, calendar);
		
		this.control.verify();
	}

	/**
	 * @see java.sql.PreparedStatement#setUnicodeStream(int, java.io.InputStream, int)
	 */
	@Test(dataProvider = "int-inputStream-int")
	@Deprecated
	public void setUnicodeStream(int index, InputStream inputStream, int length) throws SQLException
	{
		File file = new File("");
		InputStream input = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.fileSupport.createFile(inputStream)).andReturn(file);
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(input);
		
		this.getSQLStatement().setUnicodeStream(index, input, length);
		
		this.control.replay();

		this.getStatement().setUnicodeStream(index, inputStream, length);
		
		this.control.verify();
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
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		this.getSQLStatement().setURL(index, value);
		
		this.control.replay();

		this.getStatement().setURL(index, value);
		
		this.control.verify();
	}
}
