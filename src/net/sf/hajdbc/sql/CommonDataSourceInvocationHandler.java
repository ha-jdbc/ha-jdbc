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

import java.lang.reflect.Method;
import java.util.Set;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 * @param <D> 
 */
@SuppressWarnings("nls")
public class CommonDataSourceInvocationHandler<D> extends AbstractRootInvocationHandler<D>
{
//	private static final Set<Method> getMethodSet = Methods.findMethods(CommonDataSource.class, "get\\w+");
//	private static final Set<Method> setMethodSet = Methods.findMethods(CommonDataSource.class, "set\\w+");
	
	private final Set<Method> getMethodSet;
	private final Set<Method> setMethodSet;
	
	/**
	 * @param databaseCluster
	 * @param proxyClass
	 */
	protected CommonDataSourceInvocationHandler(DatabaseCluster<D> databaseCluster, Class<D> proxyClass)
	{
		super(databaseCluster, proxyClass);
		
		this.getMethodSet = Methods.findMethods(proxyClass, "get\\w+");
		this.setMethodSet = Methods.findMethods(proxyClass, "set\\w+");
	}

	@Override
	protected InvocationStrategy<D, D, ?> getInvocationStrategy(D object, Method method, Object[] parameters) throws Exception
	{
		if (this.getMethodSet.contains(method))
		{
			return new DriverReadInvocationStrategy<D, D, Object>();
		}

		if (this.setMethodSet.contains(method))
		{
			return new DriverWriteInvocationStrategy<D, D, Object>();
		}
		
		return super.getInvocationStrategy(object, method, parameters);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#isRecordable(java.lang.reflect.Method)
	 */
	@Override
	protected boolean isRecordable(Method method)
	{
		return this.setMethodSet.contains(method);
	}
}
