/*
 * Copyright (c) 2004, Identity Theft 911, LLC.  All rights reserved.
 *
 * $Id$
 */
package net.sf.hajdbc.balancer;

import net.sf.hajdbc.Balancer;

public class TestRoundRobinBalancer extends TestBalancer
{
	protected Balancer createBalancer()
	{
		return new RoundRobinBalancer();
	}

	protected void testNext(Balancer balancer)
	{
		int count = 100;
		
		balancer.add(new MockDatabase("0", 0));
		
		for (int i = 0; i < count; ++i)
		{
			int weight = balancer.next().getWeight().intValue();
			
			assert weight == 0;
		}
		
		balancer.add(new MockDatabase("1", 1));
		
		for (int i = 0; i < count; ++i)
		{
			int weight = balancer.next().getWeight().intValue();
			
			assert weight == 1;
		}
		
		balancer.add(new MockDatabase("2", 2));
		
		int[] expected = new int[] { 1, 2, 2 };
		
		for (int i = 0; i < count; ++i)
		{
			int weight = balancer.next().getWeight().intValue();
			
			assert weight == expected[i % 3];
		}
	}
}
