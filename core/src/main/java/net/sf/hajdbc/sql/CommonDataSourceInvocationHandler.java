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

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.InvocationStrategies;
import net.sf.hajdbc.invocation.InvocationStrategy;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 * @param <D> 
 */
@SuppressWarnings("nls")
public class CommonDataSourceInvocationHandler<Z extends javax.sql.CommonDataSource, D extends Database<Z>, F extends RootProxyFactory<Z, D>> extends ConnectionSourceInvocationHandler<Z, D, F>
{
	private static final Set<Method> getMethodSet = Methods.findMethods(CommonDataSource.class, "get\\w+");
	private static final Set<Method> setMethodSet = Methods.findMethods(CommonDataSource.class, "set\\w+");
	
	protected CommonDataSourceInvocationHandler(Class<Z> targetClass, F factory)
	{
		super(targetClass, factory);
	}

	@Override
	protected InvocationStrategy getInvocationStrategy(Z dataSource, Method method, Object... parameters) throws SQLException
	{
		if (getMethodSet.contains(method))
		{
			return InvocationStrategies.INVOKE_ON_ANY;
		}

		if (setMethodSet.contains(method))
		{
			return InvocationStrategies.INVOKE_ON_EXISTING;
		}
		
		return super.getInvocationStrategy(dataSource, method, parameters);
	}

	@Override
	protected <R> void postInvoke(Invoker<Z, D, Z, R, SQLException> invoker, Z proxy, Method method, Object... parameters)
	{
		if (setMethodSet.contains(method))
		{
			this.getProxyFactory().record(invoker);
		}
	}
}
