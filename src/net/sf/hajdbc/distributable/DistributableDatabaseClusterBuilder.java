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

import org.jgroups.Channel;
import org.jgroups.JChannelFactory;

/**
 * @author  Paul Ferraro
 * @since   1.1
 */
public class DistributableDatabaseClusterBuilder implements DatabaseClusterBuilder
{
	private JChannelFactory factory = new JChannelFactory();
	private String config;
	private String stack;
	private int timeout;
	
	/**
	 * Returns the name of the protocol stack.
	 * @return a stack name
	 */
	public String getStack()
	{
		return this.stack;
	}

	/**
	 * Sets the name of the protocol stack.
	 * @param stack a stack name
	 */
	public void setStack(String stack)
	{
		this.stack = stack;
	}

	/**
	 * Returns the configuration of protocol stacks.
	 * @return a configuration of protocol stacks.
	 */
	public String getConfig()
	{
		return this.config;
	}
	
	/**
	 * Sets the configuration of protocol stacks.
	 * @param config  a configuration of protocol stacks.
	 */
	public void setConfig(String config) throws Exception
	{
		this.config = config;
		
		this.factory.setDomain("org.jgroups");
		this.factory.create();
		this.factory.setMultiplexerConfig(config);
	}
	
	public Channel getChannel(String id) throws Exception
	{
		return this.factory.createMultiplexerChannel(this.stack, id);
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
	 * @param timeout
	 */
	public void setTimeout(int timeout)
	{
		this.timeout = timeout;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterBuilder#buildDatabaseCluster()
	 */
	public DatabaseCluster buildDatabaseCluster()
	{
		return new DistributableDatabaseCluster(this);
	}
}
