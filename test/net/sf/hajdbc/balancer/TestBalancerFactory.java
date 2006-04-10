/*
 * Copyright (c) 2004-2006, Identity Theft 911, LLC.  All rights reserved.
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
