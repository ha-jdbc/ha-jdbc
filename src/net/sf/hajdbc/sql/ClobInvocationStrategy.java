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

import java.sql.Clob;

import net.sf.hajdbc.util.reflect.ProxyFactory;

/**
 * @author Paul Ferraro
 *
 */
public class ClobInvocationStrategy<D, P> extends DatabaseWriteInvocationStrategy<D, P, Clob>
{
	private P parent;
	private Class<? extends Clob> clobClass;
	
	public ClobInvocationStrategy(P parent, Class<? extends Clob> clobClass)
	{
		super(null);
		
		this.parent = parent;
		this.clobClass = clobClass;
	}

	/**
	 * @see net.sf.hajdbc.sql.DatabaseWriteInvocationStrategy#invoke(net.sf.hajdbc.sql.SQLProxy, net.sf.hajdbc.sql.Invoker)
	 */
	@Override
	public Clob invoke(SQLProxy<D, P> proxy, Invoker<D, P, Clob> invoker) throws Exception
	{
		return ProxyFactory.createProxy(this.clobClass, new ClobInvocationHandler<D, P>(this.parent, proxy, invoker, this.invokeAll(proxy, invoker)));
	}
}
