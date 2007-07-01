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
package net.sf.hajdbc.distributable;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseClusterDecorator;

import org.jgroups.Channel;
import org.jgroups.JChannelFactory;

/**
 * @author Paul Ferraro
 *
 */
public class DistributableDatabaseClusterDecorator implements DatabaseClusterDecorator
{
	private JChannelFactory factory;
	private String config = "stacks.xml";
	private String stack = "udp-sync";
	private int timeout = 1000;
	
	public Channel createChannel(String name) throws Exception
	{
		return this.factory.createMultiplexerChannel(this.stack, name);
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
	 * @see net.sf.hajdbc.DatabaseClusterDecorator#decorate(net.sf.hajdbc.DatabaseCluster)
	 */
	public <D> void decorate(DatabaseCluster<D> databaseCluster) throws Exception
	{
		if (this.factory == null)
		{
			this.factory = new JChannelFactory();
			
			this.factory.setDomain("org.jgroups");
			this.factory.setMultiplexerConfig(this.config);
			this.factory.create();
		}
		
		databaseCluster.setLockManager(new DistributableLockManager(databaseCluster, this));
		databaseCluster.setStateManager(new DistributableStateManager(databaseCluster, this));
	}

	@Override
	protected void finalize()
	{
		this.factory.destroy();
	}
}
