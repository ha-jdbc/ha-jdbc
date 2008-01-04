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

import java.sql.SQLException;
import java.util.Map;

import net.sf.hajdbc.Database;

/**
 * @author Paul Ferraro
 *
 */
public abstract class AbstractChildInvocationHandler<D, P, E> extends AbstractInvocationHandler<D, E>
{
	private P parentObject;
	private SQLProxy<D, P> parentProxy;
	private Invoker<D, P, E> parentInvoker;

	protected AbstractChildInvocationHandler(P object, SQLProxy<D, P> proxy, Invoker<D, P, E> invoker, Class<E> proxyClass, Map<Database<D>, E> objectMap) throws Exception
	{
		super(proxy.getDatabaseCluster(), proxyClass, objectMap);
		
		this.parentObject = object;
		this.parentProxy = proxy;
		this.parentInvoker = invoker;
		this.parentProxy.addChild(this);
	}
	
	@Override
	protected E createObject(Database<D> database) throws SQLException
	{
		P parentObject = this.parentProxy.getObject(database);
		
		if (parentObject == null)
		{
			throw new IllegalStateException();
		}
		
		return this.parentInvoker.invoke(database, parentObject);
	}

	@Override
	protected void close(Database<D> database, E object)
	{
		try
		{
			this.close(this.parentProxy.getObject(database), object);
		}
		catch (SQLException e)
		{
			this.logger.info(e.getMessage(), e);
		}
	}
	
	protected abstract void close(P parent, E object) throws SQLException;
	
	/**
	 * @see net.sf.hajdbc.sql.SQLProxy#getRoot()
	 */
	@Override
	public final SQLProxy<D, ?> getRoot()
	{
		return this.parentProxy.getRoot();
	}
	
	protected P getParent()
	{
		return this.parentObject;
	}
	
	protected SQLProxy<D, P> getParentProxy()
	{
		return this.parentProxy;
	}
}
