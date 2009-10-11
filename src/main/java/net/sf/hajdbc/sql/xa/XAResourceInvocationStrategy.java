/*
 * HA-JDBC: High-Availability JDBC
 * Copyright 2004-2009 Paul Ferraro
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc.sql.xa;

import java.sql.SQLException;

import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAResource;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.sql.DatabaseWriteInvocationStrategy;
import net.sf.hajdbc.sql.Invoker;
import net.sf.hajdbc.sql.SQLProxy;
import net.sf.hajdbc.util.reflect.ProxyFactory;

/**
 * @author Paul Ferraro
 *
 */
public class XAResourceInvocationStrategy extends DatabaseWriteInvocationStrategy<XADataSource, XADataSourceDatabase, XAConnection, XAResource, SQLException>
{
	private XAConnection connection;

	/**
	 * @param cluster 
	 * @param connection
	 */
	public XAResourceInvocationStrategy(DatabaseCluster<XADataSource, XADataSourceDatabase> cluster, XAConnection connection)
	{
		super(cluster.getNonTransactionalExecutor());
		
		this.connection = connection;
	}

	/**
	 * @see net.sf.hajdbc.sql.DatabaseWriteInvocationStrategy#invoke(net.sf.hajdbc.sql.SQLProxy, net.sf.hajdbc.sql.Invoker)
	 */
	@Override
	public XAResource invoke(SQLProxy<XADataSource, XADataSourceDatabase, XAConnection, SQLException> proxy, Invoker<XADataSource, XADataSourceDatabase, XAConnection, XAResource, SQLException> invoker) throws SQLException
	{
		return ProxyFactory.createProxy(XAResource.class, new XAResourceInvocationHandler(this.connection, proxy, invoker, this.invokeAll(proxy, invoker)));
	}
}
