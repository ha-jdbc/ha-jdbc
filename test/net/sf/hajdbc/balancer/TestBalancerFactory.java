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
package net.sf.hajdbc.balancer;

import net.sf.hajdbc.Balancer;

import org.easymock.EasyMock;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
@Test
@SuppressWarnings("unchecked")
public class TestBalancerFactory
{
	public void testSerialize()
	{
		this.assertBalancer(new SimpleBalancer(), "simple");
		this.assertBalancer(new RandomBalancer(), "random");
		this.assertBalancer(new RoundRobinBalancer(), "round-robin");
		this.assertBalancer(new LoadBalancer(), "load");
		
		try
		{
			String balancer = BalancerFactory.serialize(EasyMock.createMock(Balancer.class));
			
			assert false : balancer;
		}
		catch (IllegalArgumentException e)
		{
			assert true;
		}
	}
	
	private void assertBalancer(Balancer balancer, String id)
	{
		String name = BalancerFactory.serialize(balancer);
		
		assert name.equals(id) : name;
	}
	
	public void testDeserialize()
	{
		this.assertBalancer("simple", SimpleBalancer.class);
		this.assertBalancer("random", RandomBalancer.class);
		this.assertBalancer("round-robin", RoundRobinBalancer.class);
		this.assertBalancer("load", LoadBalancer.class);
			
		try
		{
			Balancer balancer = BalancerFactory.deserialize("invalid");
			
			assert false : balancer.getClass().getName();
		}
		catch (Exception e)
		{
			assert true;
		}
	}
	
	private void assertBalancer(String id, Class<? extends Balancer> balancerClass)
	{
		try
		{
			Balancer balancer = BalancerFactory.deserialize("load");
			
			assert LoadBalancer.class.isInstance(balancer);
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}
}
