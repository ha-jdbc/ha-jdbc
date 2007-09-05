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
import java.sql.Blob;
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
public class TestBlob implements Blob
{
	private Balancer balancer = EasyMock.createStrictMock(Balancer.class);
	private DatabaseCluster cluster = EasyMock.createStrictMock(DatabaseCluster.class);
	private Lock readLock = EasyMock.createStrictMock(Lock.class);
	private Lock writeLock1 = EasyMock.createStrictMock(Lock.class);
	private Lock writeLock2 = EasyMock.createStrictMock(Lock.class);
	private LockManager lockManager = EasyMock.createStrictMock(LockManager.class);
	private Blob blob1 = EasyMock.createStrictMock(java.sql.Blob.class);
	private Blob blob2 = EasyMock.createStrictMock(java.sql.Blob.class);
	private SQLProxy parent = EasyMock.createStrictMock(SQLProxy.class);
	private SQLProxy root = EasyMock.createStrictMock(SQLProxy.class);
	
	private Database database1 = new MockDatabase("1");
	private Database database2 = new MockDatabase("2");
	private Set<Database> databaseSet;
	private ExecutorService executor = Executors.newSingleThreadExecutor();
	private Blob blob;
	private BlobInvocationHandler handler;
	
	@BeforeClass
	void init() throws Exception
	{
		Map<Database, Blob> map = new TreeMap<Database, Blob>();
		map.put(this.database1, this.blob1);
		map.put(this.database2, this.blob2);
		
		this.databaseSet = map.keySet();
		
		EasyMock.expect(this.parent.getDatabaseCluster()).andReturn(this.cluster);
		
		this.parent.addChild(EasyMock.isA(BlobInvocationHandler.class));
		
		this.replay();
		
		this.handler = new BlobInvocationHandler(new Object(), this.parent, EasyMock.createMock(Invoker.class), map);
		this.blob = ProxyFactory.createProxy(Blob.class, this.handler);
		
		this.verify();
		this.reset();
	}
	
	private Object[] objects()
	{
		return new Object[] { this.cluster, this.balancer, this.blob1, this.blob2, this.readLock, this.writeLock1, this.writeLock2, this.lockManager, this.parent, this.root };
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
	 * @see java.sql.Blob#free()
	 */
	@Test
	public void free() throws SQLException
	{
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);

		this.root.retain(this.databaseSet);
		
		this.parent.removeChild(this.handler);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		this.blob1.free();
		this.blob2.free();
		
		this.replay();
		
		this.blob.free();
		
		this.verify();
	}

	/**
	 * @see java.sql.Blob#getBinaryStream()
	 */
	@Test
	public InputStream getBinaryStream() throws SQLException
	{
		InputStream input = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database2);
		
		this.balancer.beforeInvocation(this.database2);
		
		EasyMock.expect(this.blob2.getBinaryStream()).andReturn(input);

		this.balancer.afterInvocation(this.database2);
		
		this.replay();
		
		InputStream result = this.blob.getBinaryStream();
		
		this.verify();
		
		assert result == input;
		
		return result;
	}

	@DataProvider(name = "long-long")
	Object[][] longLongProvider()
	{
		return new Object[][] { new Object[] { 1L, 1L } };
	}
	
	/**
	 * @see java.sql.Blob#getBinaryStream(long, long)
	 */
	@Test(dataProvider = "long-long")
	public InputStream getBinaryStream(long position, long length) throws SQLException
	{
		InputStream input = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database2);
		
		this.balancer.beforeInvocation(this.database2);
		
		EasyMock.expect(this.blob2.getBinaryStream(position, length)).andReturn(input);

		this.balancer.afterInvocation(this.database2);
		
		this.replay();
		
		InputStream result = this.blob.getBinaryStream(position, length);
		
		this.verify();
		
		assert result == input;
		
		return result;
	}

	@DataProvider(name = "long-int")
	Object[][] longIntProvider()
	{
		return new Object[][] { new Object[] { 1L, 1 } };
	}

	/**
	 * @see java.sql.Blob#getBytes(long, int)
	 */
	@Test(dataProvider = "long-int")
	public byte[] getBytes(long position, int length) throws SQLException
	{
		byte[] bytes = new byte[0];
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database2);
		
		this.balancer.beforeInvocation(this.database2);
		
		EasyMock.expect(this.blob2.getBytes(position, length)).andReturn(bytes);

		this.balancer.afterInvocation(this.database2);
		
		this.replay();
		
		byte[] result = this.blob.getBytes(position, length);
		
		this.verify();
		
		assert result == bytes;
		
		return result;
	}

	/**
	 * @see java.sql.Blob#length()
	 */
	@Test
	public long length() throws SQLException
	{
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database2);
		
		this.balancer.beforeInvocation(this.database2);
		
		EasyMock.expect(this.blob2.length()).andReturn(1L);

		this.balancer.afterInvocation(this.database2);
		
		this.replay();
		
		long result = this.blob.length();
		
		this.verify();
		
		assert result == 1L;
		
		return result;
	}

	@DataProvider(name = "bytes-long")
	Object[][] bytesLongProvider()
	{
		return new Object[][] { new Object[] { new byte[0], 1L } };
	}
	
	/**
	 * @see java.sql.Blob#position(byte[], long)
	 */
	@Test(dataProvider = "bytes-long")
	public long position(byte[] pattern, long start) throws SQLException
	{
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database2);
		
		this.balancer.beforeInvocation(this.database2);
		
		EasyMock.expect(this.blob2.position(pattern, start)).andReturn(1L);

		this.balancer.afterInvocation(this.database2);
		
		this.replay();
		
		long result = this.blob.position(pattern, start);
		
		this.verify();
		
		assert result == 1L;
		
		return result;
	}

	@DataProvider(name = "blob-long")
	Object[][] blobLongProvider()
	{
		return new Object[][] { new Object[] { EasyMock.createMock(Blob.class), 1L } };
	}

	/**
	 * @see java.sql.Blob#position(java.sql.Blob, long)
	 */
	@Test(dataProvider = "blob-long")
	public long position(Blob pattern, long start) throws SQLException
	{
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database2);
		
		this.balancer.beforeInvocation(this.database2);
		
		EasyMock.expect(this.blob2.position(pattern, start)).andReturn(1L);

		this.balancer.afterInvocation(this.database2);
		
		this.replay();
		
		long result = this.blob.position(pattern, start);
		
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
	 * @see java.sql.Blob#setBinaryStream(long)
	 */
	@Test(dataProvider = "long")
	public OutputStream setBinaryStream(long position) throws SQLException
	{
		OutputStream output1 = new ByteArrayOutputStream();
		OutputStream output2 = new ByteArrayOutputStream();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);

		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.blob1.setBinaryStream(position)).andReturn(output1);
		EasyMock.expect(this.blob2.setBinaryStream(position)).andReturn(output2);
		
		this.replay();
		
		OutputStream result = this.blob.setBinaryStream(position);

		this.verify();
		
		assert result == output1;
		
		return result;
	}

	@DataProvider(name = "long-bytes")
	Object[][] longBytesProvider()
	{
		return new Object[][] { new Object[] { 1L, new byte[0] } };
	}
	
	/**
	 * @see java.sql.Blob#setBytes(long, byte[])
	 */
	@Test(dataProvider = "long-bytes")
	public int setBytes(long position, byte[] bytes) throws SQLException
	{
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);

		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.blob1.setBytes(position, bytes)).andReturn(1);
		EasyMock.expect(this.blob2.setBytes(position, bytes)).andReturn(1);
		
		this.replay();
		
		int result = this.blob.setBytes(position, bytes);

		this.verify();
		
		assert result == 1;
		
		return result;
	}

	@DataProvider(name = "long-bytes-int-int")
	Object[][] longBytesIntIntProvider()
	{
		return new Object[][] { new Object[] { 1L, new byte[0], 1, 1 } };
	}

	/**
	 * @see java.sql.Blob#setBytes(long, byte[], int, int)
	 */
	@Test(dataProvider = "long-bytes-int-int")
	public int setBytes(long position, byte[] bytes, int offset, int length) throws SQLException
	{
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);

		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.blob1.setBytes(position, bytes, offset, length)).andReturn(1);
		EasyMock.expect(this.blob2.setBytes(position, bytes, offset, length)).andReturn(1);
		
		this.replay();
		
		int result = this.blob.setBytes(position, bytes, offset, length);

		this.verify();
		
		assert result == 1;
		
		return result;
	}

	/**
	 * @see java.sql.Blob#truncate(long)
	 */
	@Test(dataProvider = "long")
	public void truncate(long position) throws SQLException
	{
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		this.blob1.truncate(position);
		this.blob2.truncate(position);
		
		this.replay();
		
		this.blob.truncate(position);

		this.verify();
	}
}
