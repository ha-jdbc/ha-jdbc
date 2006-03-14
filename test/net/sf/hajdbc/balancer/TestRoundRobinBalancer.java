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

import net.sf.hajdbc.Balancer;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class TestRoundRobinBalancer extends AbstractTestBalancer
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
			int weight = balancer.next().getWeight();
			
			assertEquals(0, weight);
		}
		
		balancer.add(new MockDatabase("1", 1));
		
		for (int i = 0; i < count; ++i)
		{
			int weight = balancer.next().getWeight();
			
			assertEquals(1, weight);
		}
		
		balancer.add(new MockDatabase("2", 2));
		
		int[] expected = new int[] { 1, 2, 2 };
		
		for (int i = 0; i < count; ++i)
		{
			int weight = balancer.next().getWeight();
			
			assertEquals(expected[i % 3], weight);
		}
	}
}
