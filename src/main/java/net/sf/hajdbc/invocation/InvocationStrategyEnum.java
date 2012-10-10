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
package net.sf.hajdbc.invocation;

import java.util.SortedMap;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.sql.SQLProxy;

public enum InvocationStrategyEnum implements InvocationStrategy
{
	INVOKE_ON_ALL(new InvokeOnAllInvocationStrategy()),
	INVOKE_ON_ANY(new InvokeOnAnyInvocationStrategy()),
	INVOKE_ON_EXISTING(new InvokeOnExistingInvocationStrategy()),
	INVOKE_ON_NEXT(new InvokeOnNextInvocationStrategy()),
	INVOKE_ON_PRIMARY(new InvokeOnPrimaryInvocationStrategy()),
	TRANSACTION_INVOKE_ON_ALL(new TransactionInvokeOnAllInvocationStrategy(false)),
	END_TRANSACTION_INVOKE_ON_ALL(new TransactionInvokeOnAllInvocationStrategy(true)),
	;

	private final InvocationStrategy strategy;
	
	private InvocationStrategyEnum(InvocationStrategy strategy)
	{
		this.strategy = strategy;
	}
	
	@Override
	public <Z, D extends Database<Z>, T, R, E extends Exception> SortedMap<D, R> invoke(SQLProxy<Z, D, T, E> proxy, Invoker<Z, D, T, R, E> invoker) throws E
	{
		return this.strategy.invoke(proxy, invoker);
	}
}
