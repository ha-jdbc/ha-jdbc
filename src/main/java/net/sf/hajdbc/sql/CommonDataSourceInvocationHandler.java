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
import java.sql.SQLException;
import java.util.Set;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.ExceptionFactory;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 * @param <D> 
 */
@SuppressWarnings("nls")
public class CommonDataSourceInvocationHandler<Z extends javax.sql.CommonDataSource, D extends CommonDataSourceDatabase<Z>> extends AbstractRootInvocationHandler<Z, D, SQLException>
{
	private static final Set<Method> getMethodSet = Methods.findMethods(CommonDataSource.class, "get\\w+");
	private static final Set<Method> setMethodSet = Methods.findMethods(CommonDataSource.class, "set\\w+");
	
	/**
	 * @param databaseCluster
	 * @param proxyClass
	 */
	protected CommonDataSourceInvocationHandler(DatabaseCluster<Z, D> databaseCluster, Class<Z> proxyClass)
	{
		super(databaseCluster, proxyClass);
	}

	@Override
	protected InvocationStrategy<Z, D, Z, ?, SQLException> getInvocationStrategy(Z object, Method method, Object[] parameters) throws SQLException
	{
		if (getMethodSet.contains(method))
		{
			return new DriverReadInvocationStrategy<Z, D, Z, Object, SQLException>();
		}

		if (setMethodSet.contains(method))
		{
			return new DriverWriteInvocationStrategy<Z, D, Z, Object, SQLException>();
		}
		
		return super.getInvocationStrategy(object, method, parameters);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#isRecordable(java.lang.reflect.Method)
	 */
	@Override
	protected boolean isRecordable(Method method)
	{
		return setMethodSet.contains(method);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.SQLProxy#getExceptionFactory()
	 */
	@Override
	public ExceptionFactory<SQLException> getExceptionFactory()
	{
		return SQLExceptionFactory.getInstance();
	}
}