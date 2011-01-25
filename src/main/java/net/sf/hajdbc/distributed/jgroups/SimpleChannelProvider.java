/*
 * HA-JDBC: High-Availability JDBC
 * Copyright 2004-2009 Paul Ferraro
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
package net.sf.hajdbc.distributed.jgroups;

import java.util.concurrent.TimeUnit;

import org.jgroups.Channel;

/**
 * Simple channel provider to support channel injection.
 * 
 * @author Paul Ferraro
 */
public class SimpleChannelProvider extends ChannelCommandDispatcherFactory
{
	private final Channel channel;
	private volatile long timeout = DEFAULT_TIMEOUT;
	
	public SimpleChannelProvider(Channel channel)
	{
		this.channel = channel;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.distributed.jgroups.ChannelProvider#getChannel()
	 */
	@Override
	public Channel getChannel()
	{
		return this.channel;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.distributed.jgroups.ChannelProvider#getTimeout()
	 */
	@Override
	public long getTimeout()
	{
		return this.timeout;
	}
	
	public void setTimeout(long timeout)
	{
		this.timeout = timeout;
	}
	
	public void setTimeout(int timeout, TimeUnit unit)
	{
		this.setTimeout(unit.toMillis(timeout));
	}
}
