/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
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

import java.sql.SQLException;

import net.sf.hajdbc.SynchronizationContext;
import net.sf.hajdbc.SynchronizationStrategy;

/**
 * @author Paul Ferraro
 *
 */
public abstract class LockingSynchronizationStrategy implements SynchronizationStrategy
{
	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#cleanup(net.sf.hajdbc.SynchronizationContext)
	 */
	public void cleanup(SynchronizationContext context)
	{
		SynchronizationSupport.unlock(context);
	}

	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#prepare(net.sf.hajdbc.SynchronizationContext)
	 */
	public void prepare(SynchronizationContext context) throws SQLException
	{
		SynchronizationSupport.lock(context);
	}
}