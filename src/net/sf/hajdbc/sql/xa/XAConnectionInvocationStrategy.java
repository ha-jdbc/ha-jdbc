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

import javax.sql.XAConnection;
import javax.sql.XADataSource;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.sql.DatabaseWriteInvocationStrategy;
import net.sf.hajdbc.sql.Invoker;
import net.sf.hajdbc.sql.SQLProxy;
import net.sf.hajdbc.util.reflect.ProxyFactory;

/**
 * @author Paul Ferraro
 *
 */
public class XAConnectionInvocationStrategy extends DatabaseWriteInvocationStrategy<XADataSource, XADataSource, XAConnection>
{
	private XADataSource dataSource;

	/**
	 * @param cluster 
	 * @param dataSource
	 */
	public XAConnectionInvocationStrategy(DatabaseCluster<XADataSource> cluster, XADataSource dataSource)
	{
		super(cluster.getNonTransactionalExecutor());
		
		this.dataSource = dataSource;
	}

	@Override
	public XAConnection invoke(SQLProxy<XADataSource, XADataSource> proxy, Invoker<XADataSource, XADataSource, XAConnection> invoker) throws Exception
	{
		return ProxyFactory.createProxy(XAConnection.class, new XAConnectionInvocationHandler(this.dataSource, proxy, invoker, this.invokeAll(proxy, invoker)));
	}
}
