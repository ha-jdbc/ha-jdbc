/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2009 Paul Ferraro
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
package net.sf.hajdbc.distributed.jgroups;


import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import org.jgroups.Channel;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.conf.ConfiguratorFactory;
import org.jgroups.conf.ProtocolData;
import org.jgroups.conf.ProtocolParameter;
import org.jgroups.conf.ProtocolStackConfigurator;

/**
 * @author paul
 *
 */
@XmlType
@XmlAccessorType(XmlAccessType.FIELD)
public class DefaultChannelProvider implements ChannelProvider
{
	@XmlTransient
	private static final String SINGLETON_NAME = "ha-jdbc";
	
	@XmlAttribute(name = "stack")
	private String stack = "udp-sync.xml"; //$NON-NLS-1$
	
	public String getStack()
	{
		return this.stack;
	}
	
	public void setStack(String stack)
	{
		this.stack = stack;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.distributed.jgroups.ChannelProvider#getChannel()
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Channel getChannel() throws Exception
	{
		ProtocolStackConfigurator configurator = ConfiguratorFactory.getStackConfigurator(this.stack);
		
		// Use shared transport for all ha-jdbc channels
		ProtocolData transport = configurator.getProtocolStack()[0];

		ProtocolParameter parameter = new ProtocolParameter(Global.SINGLETON_NAME, SINGLETON_NAME);
		
		transport.getParameters().put(parameter.getName(), parameter);
		
		return new JChannel(configurator);
	}
}
