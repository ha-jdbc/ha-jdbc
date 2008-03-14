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
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.LockManager;
import net.sf.hajdbc.MockDatabase;
import net.sf.hajdbc.util.reflect.ProxyFactory;

import org.easymock.EasyMock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
@SuppressWarnings({ "unchecked", "nls" })
public class TestClob implements NClob
{
	private Balancer balancer = EasyMock.createStrictMock(Balancer.class);
	private DatabaseCluster cluster = EasyMock.createStrictMock(DatabaseCluster.class);
	private Lock readLock = EasyMock.createStrictMock(Lock.class);
	private Lock writeLock1 = EasyMock.createStrictMock(Lock.class);
	private Lock writeLock2 = EasyMock.createStrictMock(Lock.class);
	private LockManager lockManager = EasyMock.createStrictMock(LockManager.class);
	private NClob clob1 = EasyMock.createStrictMock(NClob.class);
	private NClob clob2 = EasyMock.createStrictMock(NClob.class);
	private SQLProxy parent = EasyMock.createStrictMock(SQLProxy.class);
	private SQLProxy root = EasyMock.createStrictMock(SQLProxy.class);
	
	private Database database1 = new MockDatabase("1");
	private Database database2 = new MockDatabase("2");
	private Set<Database> databaseSet;
	private ExecutorService executor = Executors.newSingleThreadExecutor();
	private NClob clob;
	private ClobInvocationHandler handler;
	
	@BeforeClass
	void init() throws Exception
	{
		Map<Database, NClob> map = new TreeMap<Database, NClob>();
		map.put(this.database1, this.clob1);
		map.put(this.database2, this.clob2);
		
		this.databaseSet = map.keySet();
		
		EasyMock.expect(this.parent.getDatabaseCluster()).andReturn(this.cluster);
		this.parent.addChild(EasyMock.isA(ClobInvocationHandler.class));
		
		this.replay();
		
		this.handler = new ClobInvocationHandler(new Object(), this.parent, EasyMock.createMock(Invoker.class), map);
		this.clob = ProxyFactory.createProxy(NClob.class, this.handler);
		
		this.verify();
		this.reset();
	}
	
	private Object[] objects()
	{
		return new Object[] { this.cluster, this.balancer, this.clob1, this.clob2, this.readLock, this.writeLock1, this.writeLock2, this.lockManager, this.parent, this.root };
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

	/**
	 * @see java.sql.Clob#free()
	 */
	@Test
	public void free() throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);

		this.root.retain(this.databaseSet);
		
		this.parent.removeChild(this.handler);
		
		this.clob1.free();
		this.clob2.free();
		
		this.replay();
		
		this.clob.free();
		
		this.verify();
	}

	/**
	 * @see java.sql.Clob#getAsciiStream()
	 */
	@Test
	public InputStream getAsciiStream() throws SQLException
	{
		InputStream input = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database2);
		
		this.balancer.beforeInvocation(this.database2);
		
		EasyMock.expect(this.clob2.getAsciiStream()).andReturn(input);

		this.balancer.afterInvocation(this.database2);
		
		this.replay();
		
		InputStream result = this.clob.getAsciiStream();
		
		this.verify();
		
		assert result == input;
		
		return result;
	}

	/**
	 * @see java.sql.Clob#getCharacterStream()
	 */
	@Test
	public Reader getCharacterStream() throws SQLException
	{
		Reader reader = new StringReader("");
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database2);
		
		this.balancer.beforeInvocation(this.database2);
		
		EasyMock.expect(this.clob2.getCharacterStream()).andReturn(reader);

		this.balancer.afterInvocation(this.database2);
		
		this.replay();
		
		Reader result = this.clob.getCharacterStream();
		
		this.verify();
		
		assert result == reader;
		
		return result;
	}

	@DataProvider(name = "long-long")
	Object[][] longLongProvider()
	{
		return new Object[][] { new Object[] { 1L, 1L } };
	}

	/**
	 * @see java.sql.Clob#getCharacterStream(long, long)
	 */
	@Test(dataProvider = "long-long")
	public Reader getCharacterStream(long position, long length) throws SQLException
	{
		Reader reader = new StringReader("");
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database2);
		
		this.balancer.beforeInvocation(this.database2);
		
		EasyMock.expect(this.clob2.getCharacterStream(position, length)).andReturn(reader);

		this.balancer.afterInvocation(this.database2);
		
		this.replay();
		
		Reader result = this.clob.getCharacterStream(position, length);
		
		this.verify();
		
		assert result == reader;
		
		return result;
	}

	@DataProvider(name = "long-int")
	Object[][] longIntProvider()
	{
		return new Object[][] { new Object[] { 1L, 1 } };
	}

	/**
	 * @see java.sql.Clob#getSubString(long, int)
	 */
	@Test(dataProvider = "long-int")
	public String getSubString(long position, int length) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database2);
		
		this.balancer.beforeInvocation(this.database2);
		
		EasyMock.expect(this.clob2.getSubString(position, length)).andReturn("");

		this.balancer.afterInvocation(this.database2);
		
		this.replay();
		
		String result = this.clob.getSubString(position, length);
		
		this.verify();
		
		assert result.equals("");
		
		return result;
	}

	/**
	 * @see java.sql.Clob#length()
	 */
	@Test
	public long length() throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database2);
		
		this.balancer.beforeInvocation(this.database2);
		
		EasyMock.expect(this.clob2.length()).andReturn(1L);

		this.balancer.afterInvocation(this.database2);
		
		this.replay();
		
		long result = this.clob.length();
		
		this.verify();
		
		assert result == 1L;
		
		return result;
	}

	@DataProvider(name = "string-long")
	Object[][] stringLongProvider()
	{
		return new Object[][] { new Object[] { "", 1L } };
	}

	/**
	 * @see java.sql.Clob#position(java.lang.String, long)
	 */
	@Test(dataProvider = "string-long")
	public long position(String pattern, long start) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database2);
		
		this.balancer.beforeInvocation(this.database2);
		
		EasyMock.expect(this.clob2.position(pattern, start)).andReturn(1L);

		this.balancer.afterInvocation(this.database2);
		
		this.replay();
		
		long result = this.clob.position(pattern, start);
		
		this.verify();
		
		assert result == 1L;
		
		return result;
	}

	@DataProvider(name = "clob-long")
	Object[][] clobLongProvider()
	{
		return new Object[][] { new Object[] { EasyMock.createMock(Clob.class), 1L } };
	}

	/**
	 * @see java.sql.Clob#position(java.sql.Clob, long)
	 */
	@Test(dataProvider = "clob-long")
	public long position(Clob pattern, long start) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database2);
		
		this.balancer.beforeInvocation(this.database2);
		
		EasyMock.expect(this.clob2.position(pattern, start)).andReturn(1L);

		this.balancer.afterInvocation(this.database2);
		
		this.replay();
		
		long result = this.clob.position(pattern, start);
		
		this.verify();
		
		assert result == 1L;
		
		return result;
	}

	@DataProvider(name = "long")
	Object[][] longProvider()
	{
		return new Object[][] { new Object[] { 1L } };
	}

	/**
	 * @see java.sql.Clob#setAsciiStream(long)
	 */
	@Test(dataProvider = "long")
	public OutputStream setAsciiStream(long position) throws SQLException
	{
		OutputStream output1 = new ByteArrayOutputStream();
		OutputStream output2 = new ByteArrayOutputStream();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);

		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.clob1.setAsciiStream(position)).andReturn(output1);
		EasyMock.expect(this.clob2.setAsciiStream(position)).andReturn(output2);
		
		this.replay();
		
		OutputStream result = this.clob.setAsciiStream(position);

		this.verify();
		
		assert result == output1;
		
		return result;
	}

	/**
	 * @see java.sql.Clob#setCharacterStream(long)
	 */
	@Test(dataProvider = "long")
	public Writer setCharacterStream(long position) throws SQLException
	{
		Writer writer1 = new StringWriter();
		Writer writer2 = new StringWriter();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);

		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.clob1.setCharacterStream(position)).andReturn(writer1);
		EasyMock.expect(this.clob2.setCharacterStream(position)).andReturn(writer2);
		
		this.replay();
		
		Writer result = this.clob.setCharacterStream(position);

		this.verify();
		
		assert result == writer1;
		
		return result;
	}

	@DataProvider(name = "long-string")
	Object[][] longBytesProvider()
	{
		return new Object[][] { new Object[] { 1L, "" } };
	}

	/**
	 * @see java.sql.Clob#setString(long, java.lang.String)
	 */
	@Test(dataProvider = "long-string")
	public int setString(long position, String string) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);

		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.clob1.setString(position, string)).andReturn(1);
		EasyMock.expect(this.clob2.setString(position, string)).andReturn(1);
		
		this.replay();
		
		int result = this.clob.setString(position, string);

		this.verify();
		
		assert result == 1;
		
		return result;
	}

	@DataProvider(name = "long-string-int-int")
	Object[][] longBytesIntIntProvider()
	{
		return new Object[][] { new Object[] { 1L, "", 1, 1 } };
	}

	/**
	 * @see java.sql.Clob#setString(long, java.lang.String, int, int)
	 */
	@Test(dataProvider = "long-string-int-int")
	public int setString(long position, String string, int offset, int length) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);

		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.clob1.setString(position, string, offset, length)).andReturn(1);
		EasyMock.expect(this.clob2.setString(position, string, offset, length)).andReturn(1);
		
		this.replay();
		
		int result = this.clob.setString(position, string, offset, length);

		this.verify();
		
		assert result == 1;
		
		return result;
	}

	/**
	 * @see java.sql.Clob#truncate(long)
	 */
	@Test(dataProvider = "long")
	public void truncate(long position) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);

		this.root.retain(this.databaseSet);
		
		this.clob1.truncate(position);
		this.clob2.truncate(position);
		
		this.replay();
		
		this.clob.truncate(position);

		this.verify();
	}
}
