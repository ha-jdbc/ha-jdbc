/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2004 Paul Ferraro
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
package net.sf.hajdbc.balancer;

import java.util.HashMap;
import java.util.Map;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.Messages;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public final class BalancerEnum
{
	private static Map balancerMap = new HashMap(4);
	
	static
	{
		balancerMap.put("simple", SimpleBalancer.class);
		balancerMap.put("random", RandomBalancer.class);
		balancerMap.put("round-robin", RoundRobinBalancer.class);
		balancerMap.put("load", LoadBalancer.class);
	}

	/**
	 * Creates a new instance of the Balancer implementation indentified by the specified identifier
	 * @param id an enumerated balancer identifier
	 * @return a new Balancer instance
	 */
	public static Balancer createBalancer(String id)
	{
		Class balancerClass = (Class) balancerMap.get(id);
		
		if (balancerClass == null)
		{
			throw new IllegalArgumentException(Messages.getMessage(Messages.INVALID_BALANCER, id));
		}
		
		try
		{
			return (Balancer) balancerClass.newInstance();
		}
		catch (Exception e)
		{
			throw new IllegalArgumentException(e.getMessage());
		}
	}
	
	private BalancerEnum()
	{
		// Hide constructor
	}
}
