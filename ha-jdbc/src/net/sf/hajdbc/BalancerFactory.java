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
package net.sf.hajdbc;

import java.util.HashMap;
import java.util.Map;

import net.sf.hajdbc.balancer.LoadBalancer;
import net.sf.hajdbc.balancer.RoundRobinBalancer;
import net.sf.hajdbc.balancer.RandomBalancer;
import net.sf.hajdbc.balancer.SimpleBalancer;

/**
 * @author  Paul Ferraro
 * @since   3.1
 */
public final class BalancerFactory
{
	private static Map balancerMap = getMap();
	
	private static Map getMap()
	{
		Map map = new HashMap();
		
		map.put("simple", SimpleBalancer.class);
		map.put("random", RandomBalancer.class);
		map.put("round-robin", RoundRobinBalancer.class);
		map.put("load", LoadBalancer.class);

		return map;
	}
	
	public static Balancer getBalancer(String id) throws java.sql.SQLException
	{
		Class balancerClass = (Class) balancerMap.get(id);
		
		if (balancerClass == null)
		{
			throw new SQLException("");
		}
		
		try
		{
			return (Balancer) balancerClass.newInstance();
		}
		catch (InstantiationException e)
		{
			throw new SQLException(e);
		}
		catch (IllegalAccessException e)
		{
			throw new SQLException(e);
		}
	}

	private BalancerFactory()
	{
		
	}
}
