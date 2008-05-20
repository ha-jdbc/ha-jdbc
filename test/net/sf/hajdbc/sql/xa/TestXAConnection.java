/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2008 Paul Ferraro
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
package net.sf.hajdbc.sql.xa;

import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.util.Map;

import javax.sql.PooledConnection;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAResource;

import net.sf.hajdbc.sql.Invoker;
import net.sf.hajdbc.sql.SQLProxy;
import net.sf.hajdbc.sql.pool.AbstractPooledConnectionInvocationHandler;
import net.sf.hajdbc.sql.pool.AbstractTestPooledConnection;

import org.easymock.EasyMock;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
@Test
@SuppressWarnings("unchecked")
public class TestXAConnection extends AbstractTestPooledConnection<XAConnection> implements XAConnection
{
	/**
	 * @see net.sf.hajdbc.sql.pool.AbstractTestPooledConnection#getConnectionClass()
	 */
	@Override
	protected Class<XAConnection> getConnectionClass()
	{
		return XAConnection.class;
	}

	/**
	 * @see net.sf.hajdbc.sql.pool.AbstractTestPooledConnection#getInvocationHandler(java.util.Map)
	 */
	@Override
	protected AbstractPooledConnectionInvocationHandler getInvocationHandler(Map map) throws Exception
	{
		return new XAConnectionInvocationHandler(EasyMock.createStrictMock(XADataSource.class), this.parent, EasyMock.createMock(Invoker.class), map);
	}

	public void testGetXAResource() throws SQLException
	{
		XAResource resource1 = EasyMock.createStrictMock(XAResource.class);
		XAResource resource2 = EasyMock.createStrictMock(XAResource.class);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.connection1.getXAResource()).andReturn(resource1);
		EasyMock.expect(this.connection2.getXAResource()).andReturn(resource2);
		
		this.replay();
		
		XAResource result = this.getXAResource();
		
		this.verify();
		
		assert Proxy.isProxyClass(result.getClass());
		
		SQLProxy proxy = SQLProxy.class.cast(Proxy.getInvocationHandler(result));
		
		assert proxy.getObject(this.database1) == resource1;
		assert proxy.getObject(this.database2) == resource2;
	}
	
	/**
	 * @see javax.sql.XAConnection#getXAResource()
	 */
	@Override
	public XAResource getXAResource() throws SQLException
	{
		return this.connection.getXAResource();
	}
}
