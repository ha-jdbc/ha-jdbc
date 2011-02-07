/*
 * HA-JDBC: High-Availablity JDBC
 * Copyright 2004-Apr 9, 2010 Paul Ferraro
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

import net.sf.hajdbc.Database;
import net.sf.hajdbc.balancer.Balancer;

/**
 * @author paul
 *
 */
public class InvokeOnMasterInvocationStrategy extends InvokeOnOneInvocationStrategy
{
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.invocation.InvokeOnOneInvocationStrategy#getTarget(net.sf.hajdbc.balancer.Balancer)
	 */
	@Override
	protected <Z, D extends Database<Z>> D getTarget(Balancer<Z, D> balancer)
	{
		return balancer.primary();
	}
}
