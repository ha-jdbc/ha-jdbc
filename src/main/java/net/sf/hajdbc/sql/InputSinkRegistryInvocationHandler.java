/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2013  Paul Ferraro
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

import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.ExceptionFactory;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.io.InputSinkChannel;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * 
 * @author Paul Ferraro
 */
public class InputSinkRegistryInvocationHandler<Z, D extends Database<Z>, P, T, F extends InputSinkRegistryProxyFactory<Z, D, P, T>> extends ChildInvocationHandler<Z, D, P, SQLException, T, SQLException, F>
{
	protected InputSinkRegistryInvocationHandler(Class<T> proxyClass, F proxyFactory, Method parentMethod)
	{
		super(proxyClass, proxyFactory, parentMethod);
	}

	@Override
	protected <R, X> Invoker<Z, D, T, R, SQLException> getInvoker(Class<X> parameterClass, final int parameterIndex, T proxy, final Method method, final Object... parameters) throws SQLException
	{
		if (parameterClass.equals(method.getParameterTypes()[parameterIndex]) && !parameterClass.isPrimitive())
		{
			X parameter = parameterClass.cast(parameters[parameterIndex]);
			
			if (parameter != null)
			{
				final InputSinkChannel<X, Object> channel = this.getProxyFactory().getInputSinkRegistry().get(parameterClass);
				
				if (channel != null)
				{
					final ExceptionFactory<SQLException> exceptionFactory = this.getProxyFactory().getExceptionFactory();
					try
					{
						final Object sink = channel.write(parameter);
						
						return new Invoker<Z, D, T, R, SQLException>()
						{
							@Override
							public R invoke(D database, T object) throws SQLException
							{
								List<Object> parameterList = new ArrayList<>(Arrays.asList(parameters));
								
								try
								{
									parameterList.set(parameterIndex, channel.read(sink));
									
									return Methods.<R, SQLException>invoke(method, exceptionFactory, object, parameterList.toArray());
								}
								catch (IOException e)
								{
									throw exceptionFactory.createException(e);
								}
							}
						};
					}
					catch (IOException e)
					{
						throw exceptionFactory.createException(e);
					}
				}
			}
		}
		
		return super.getInvoker(parameterClass, parameterIndex, proxy, method, parameters);
	}
}
