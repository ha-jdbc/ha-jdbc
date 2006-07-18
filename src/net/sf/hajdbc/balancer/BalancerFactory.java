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
package net.sf.hajdbc.balancer;

import java.util.HashMap;
import java.util.Map;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.Messages;

/**
 * @author  Paul Ferraro
 * @since   1.1
 */
public final class BalancerFactory
{
	private static Map<String, Class<? extends Balancer>> balancerMap = new HashMap<String, Class<? extends Balancer>>();
	
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
	 * @throws Exception if specified balancer identifier is invalid
	 */
	public static Balancer deserialize(String id) throws Exception
	{
		Class<? extends Balancer> balancerClass = balancerMap.get(id);
		
		if (balancerClass == null)
		{
			throw new IllegalArgumentException(Messages.getMessage(Messages.INVALID_BALANCER, id));
		}
		
		return balancerClass.newInstance();
	}
	
	/**
	 * Return the identifier of the specified Balancer.
	 * @param balancer a Balancer implementation
	 * @return the class name of this balancer
	 */
	public static String serialize(Balancer balancer)
	{
		for (Map.Entry<String, Class<? extends Balancer>> balancerMapEntry: balancerMap.entrySet())
		{
			if (balancerMapEntry.getValue().isInstance(balancer))
			{
				return balancerMapEntry.getKey();
			}
		}
		
		throw new IllegalArgumentException(Messages.getMessage(Messages.INVALID_BALANCER, balancer.getClass()));
	}
	
	private BalancerFactory()
	{
		// Hide constructor
	}
}
