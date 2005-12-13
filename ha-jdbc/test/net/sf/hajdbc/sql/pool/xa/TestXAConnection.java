/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2004 Paul Ferraro
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
import java.util.Collections;

import javax.transaction.xa.XAResource;

import edu.emory.mathcs.backport.java.util.concurrent.Executors;

import net.sf.hajdbc.ConnectionFactory;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.Operation;
import net.sf.hajdbc.SQLObject;
import net.sf.hajdbc.sql.pool.PooledConnection;
import net.sf.hajdbc.sql.pool.TestPooledConnection;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public class TestXAConnection extends TestPooledConnection
{
	private javax.sql.XAConnection sqlConnection = (javax.sql.XAConnection) this.sqlConnectionControl.getMock();

	protected PooledConnection createConnection() throws SQLException
	{
		ConnectionFactory connectionFactory = new ConnectionFactory(this.databaseCluster);
		
		Operation operation = new Operation()
		{
			public Object execute(Database database, Object sqlObject) throws SQLException
			{
				return sqlObject;
			}
		};
		
		return new XAConnection(connectionFactory, operation);
	}
	
	protected Class getConnectionClass()
	{
		return javax.sql.XAConnection.class;
	}

	protected XAConnection getConnection()
	{
		return (XAConnection) this.connection;
	}
	
	/**
	 * Test method for {@link XAConnection#getXAResource()}
	 */
	public void testGetXAResource()
	{
		XAResource resource1 = (XAResource) this.createMock(XAResource.class);

		this.databaseCluster.getExecutor();
		this.databaseClusterControl.setReturnValue(Executors.newSingleThreadExecutor());
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		try
		{
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlConnection.getXAResource();
			this.sqlConnectionControl.setReturnValue(resource1);
			
			this.replay();
			
			XAResource sqlResource = this.getConnection().getXAResource();
			
			this.verify();
			
			assertNotNull(sqlResource);
			assertTrue(SQLObject.class.isInstance(sqlResource));
			assertSame(resource1, ((SQLObject) sqlResource).getObject(this.database));
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}
}
