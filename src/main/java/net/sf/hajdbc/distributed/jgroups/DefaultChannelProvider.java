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

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;

import org.jgroups.Channel;
import org.jgroups.JChannel;

/**
 * Channel provider that creates a new channel from a JGroups configuration file.
 * Channels created by this provider will use a shared transport.
 * 
 * @author Paul Ferraro
 */
@XmlType
public class DefaultChannelProvider extends ChannelCommandDispatcherFactory
{
	public static final String DEFAULT_STACK = "udp-sync.xml";
	
	@XmlAttribute(name = "stack")
	private String stack = DEFAULT_STACK;
	
	@XmlAttribute(name = "timeout")
	private Long timeout = DEFAULT_TIMEOUT;
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.distributed.jgroups.ChannelProvider#getChannel()
	 */
	@Override
	public Channel getChannel() throws Exception
	{
		return new JChannel(this.stack);
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
}
