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
package net.sf.hajdbc.sql;

import java.sql.Driver;

import net.sf.hajdbc.DatabaseCluster;

/**
 * @author Paul Ferraro
 *
 */
public class DriverInvocationHandler extends AbstractInvocationHandler<Driver, Void, Driver>
{
	/**
	 * @param databaseCluster
	 */
	public DriverInvocationHandler(DatabaseCluster<Driver> databaseCluster)
	{
		super(databaseCluster, databaseCluster.getConnectionFactoryMap());
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#close(java.lang.Object, java.lang.Object)
	 */
	@Override
	protected void close(Void parent, Driver driver)
	{
		// Do nothing
	}
}
