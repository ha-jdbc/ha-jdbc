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
package net.sf.hajdbc.sql;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.util.reflect.ProxyFactory;

public class SQLXMLInvocationStrategy<D, P> extends DatabaseWriteInvocationStrategy<D, P, java.sql.SQLXML>
{
	private P parent;
	
	/**
	 * @param cluster 
	 * @param parent the object that created sql xml objects
	 */
	public SQLXMLInvocationStrategy(DatabaseCluster<D> cluster, P parent)
	{
		super(cluster.getNonTransactionalExecutor());
		
		this.parent = parent;
	}
	
	/**
	 * @see net.sf.hajdbc.sql.DatabaseWriteInvocationStrategy#invoke(net.sf.hajdbc.sql.SQLProxy, net.sf.hajdbc.sql.Invoker)
	 */
	@Override
	public java.sql.SQLXML invoke(SQLProxy<D, P> proxy, Invoker<D, P, java.sql.SQLXML> invoker) throws Exception
	{
		return ProxyFactory.createProxy(java.sql.SQLXML.class, new SQLXMLInvocationHandler<D, P>(this.parent, proxy, invoker, this.invokeAll(proxy, invoker)));
	}
}
