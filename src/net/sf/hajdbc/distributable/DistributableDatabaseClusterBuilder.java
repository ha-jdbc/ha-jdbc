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
package net.sf.hajdbc.distributable;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseClusterBuilder;

/**
 * @author  Paul Ferraro
 * @since   1.1
 */
public class DistributableDatabaseClusterBuilder implements DatabaseClusterBuilder
{
	private String protocol;
	private int timeout;
	
	/**
	 * Returns the protocol stack that this database cluster will use to broadcast cluster changes.
	 * @return a JGroups protocol stack.
	 */
	public String getProtocol()
	{
		return this.protocol;
	}
	
	/**
	 * Returns the number of milliseconds to allow for jgroups cluster operations
	 * @return a number of milliseconds
	 */
	public int getTimeout()
	{
		return this.timeout;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterBuilder#buildDatabaseCluster()
	 */
	public DatabaseCluster buildDatabaseCluster()
	{
		return new DistributableDatabaseCluster(this);
	}
}
