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
package net.sf.hajdbc.sync;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.SynchronizationStrategy;

/**
 * Trivial {@link SynchronizationStrategy} implementation that assumes that the inactive database is already in sync.
 * 
 * @author  Paul Ferraro
 * @since   1.0
 */
public class PassiveSynchronizationStrategy implements SynchronizationStrategy
{
	private static final long serialVersionUID = -7847193096593293640L;

	@Override
	public String getId()
	{
		return "passive";
	}

	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#synchronize(net.sf.hajdbc.sync.SynchronizationContext)
	 */
	@Override
	public <Z, D extends Database<Z>> void synchronize(SynchronizationContext<Z, D> context)
	{
		// Do nothing
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.SynchronizationStrategy#init(net.sf.hajdbc.DatabaseCluster)
	 */
	@Override
	public <Z, D extends Database<Z>> void init(DatabaseCluster<Z, D> cluster)
	{
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.SynchronizationStrategy#destroy(net.sf.hajdbc.DatabaseCluster)
	 */
	@Override
	public <Z, D extends Database<Z>> void destroy(DatabaseCluster<Z, D> cluster)
	{
	}
}
