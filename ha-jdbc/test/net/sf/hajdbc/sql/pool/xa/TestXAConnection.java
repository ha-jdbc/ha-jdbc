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
package net.sf.hajdbc.sql.pool.xa;

import java.sql.SQLException;
import java.util.concurrent.Executors;

import javax.transaction.xa.XAResource;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.Operation;
import net.sf.hajdbc.SQLObject;
import net.sf.hajdbc.sql.ConnectionFactory;
import net.sf.hajdbc.sql.pool.PooledConnection;
import net.sf.hajdbc.sql.pool.TestPooledConnection;

import org.easymock.EasyMock;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public class TestXAConnection extends TestPooledConnection
{
	protected PooledConnection createConnection() throws SQLException
	{
		ConnectionFactory<javax.sql.XADataSource> connectionFactory = new ConnectionFactory<javax.sql.XADataSource>(this.databaseCluster, javax.sql.XADataSource.class);
		
		Operation<javax.sql.XADataSource, javax.sql.XAConnection> operation = new Operation<javax.sql.XADataSource, javax.sql.XAConnection>()
		{
			public javax.sql.XAConnection execute(Database database, javax.sql.XADataSource dataSource) throws SQLException
			{
				return TestXAConnection.this.getSQLConnection();
			}
		};
		
		return new XAConnection(connectionFactory, operation);
	}
	
	protected Class<? extends javax.sql.PooledConnection> getConnectionClass()
	{
		return javax.sql.XAConnection.class;
	}
	
	protected javax.sql.XAConnection getSQLConnection()
	{
		return javax.sql.XAConnection.class.cast(this.sqlConnection);
	}

	protected XAConnection getConnection()
	{
		return XAConnection.class.cast(this.connection);
	}
	
	/**
	 * Test method for {@link XAConnection#getXAResource()}
	 */
	public void testGetXAResource()
	{
		XAResource sqlResource = EasyMock.createMock(XAResource.class);

		EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(Executors.newSingleThreadExecutor());
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			EasyMock.expect(this.getSQLConnection().getXAResource()).andReturn(sqlResource);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			this.control.replay();
			
			XAResource resource = this.getConnection().getXAResource();
			
			this.control.verify();
			
			assertNotNull(sqlResource);
			assertTrue(SQLObject.class.isInstance(resource));
			assertSame(sqlResource, SQLObject.class.cast(resource).getObject(this.database));
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}
}
