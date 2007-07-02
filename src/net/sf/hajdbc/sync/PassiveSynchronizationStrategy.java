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
package net.sf.hajdbc.sync;

import net.sf.hajdbc.SynchronizationContext;
import net.sf.hajdbc.SynchronizationStrategy;

/**
 * Trivial {@link SynchronizationStrategy} implementation that assumes that the inactive database is already in sync.
 * 
 * @author  Paul Ferraro
 * @since   1.0
 */
public class PassiveSynchronizationStrategy implements SynchronizationStrategy
{
	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#cleanup(net.sf.hajdbc.SynchronizationContext)
	 */
	@Override
	public <D> void cleanup(SynchronizationContext<D> context)
	{
		// Do nothing
	}

	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#prepare(net.sf.hajdbc.SynchronizationContext)
	 */
	@Override
	public <D> void prepare(SynchronizationContext<D> context)
	{
		// Do nothing
	}

	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#synchronize(net.sf.hajdbc.SynchronizationContext)
	 */
	@Override
	public <D> void synchronize(SynchronizationContext<D> context)
	{
		// Do nothing
	}
}
