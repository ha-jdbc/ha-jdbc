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

import java.util.concurrent.ExecutorService;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;

public class TransactionInvokeOnAllInvocationStrategy extends InvokeOnAllInvocationStrategy
{
	private final boolean end;
	
	public TransactionInvokeOnAllInvocationStrategy(boolean end)
	{
		this.end = end;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.invocation.InvokeOnAllInvocationStrategy#getExecutor(net.sf.hajdbc.DatabaseCluster)
	 */
	@Override
	protected <Z, D extends Database<Z>> ExecutorService getExecutor(DatabaseCluster<Z, D> cluster)
	{
		return cluster.getTransactionMode().getTransactionExecutor(cluster.getExecutor(), this.end);
	}
}
