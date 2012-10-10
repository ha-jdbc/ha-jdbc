/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
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
package net.sf.hajdbc.sql;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Set;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.invocation.InvocationStrategy;
import net.sf.hajdbc.invocation.InvocationStrategyEnum;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 * @param <D> 
 */
@SuppressWarnings("nls")
public class CommonDataSourceInvocationHandler<Z extends javax.sql.CommonDataSource, D extends CommonDataSourceDatabase<Z>> extends RootInvocationHandler<Z, D, SQLException>
{
	private static final Set<Method> getMethodSet = Methods.findMethods(CommonDataSource.class, "get\\w+");
	private static final Set<Method> setMethodSet = Methods.findMethods(CommonDataSource.class, "set\\w+");
	
	/**
	 * @param databaseCluster
	 * @param proxyClass
	 */
	protected CommonDataSourceInvocationHandler(DatabaseCluster<Z, D> databaseCluster, Class<Z> proxyClass)
	{
		super(databaseCluster, proxyClass, SQLException.class);
	}

	@Override
	protected InvocationStrategy getInvocationStrategy(Z dataSource, Method method, Object[] parameters) throws SQLException
	{
		if (getMethodSet.contains(method))
		{
			return InvocationStrategyEnum.INVOKE_ON_ANY;
		}

		if (setMethodSet.contains(method))
		{
			return InvocationStrategyEnum.INVOKE_ON_EXISTING;
		}
		
		return super.getInvocationStrategy(dataSource, method, parameters);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#isRecordable(java.lang.reflect.Method)
	 */
	@Override
	protected boolean isRecordable(Method method)
	{
		return setMethodSet.contains(method);
	}
}
