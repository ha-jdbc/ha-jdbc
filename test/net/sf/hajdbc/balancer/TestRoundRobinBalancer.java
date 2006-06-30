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

import org.testng.annotations.Test;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.MockDatabase;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
@Test
public class TestRoundRobinBalancer extends AbstractTestBalancer
{
	/**
	 * @see net.sf.hajdbc.balancer.AbstractTestBalancer#createBalancer()
	 */
	@Override
	protected Balancer createBalancer()
	{
		return new RoundRobinBalancer();
	}

	@Override
	protected void next(Balancer balancer)
	{
		int count = 100;
		
		balancer.add(new MockDatabase("0", 0));
		
		for (int i = 0; i < count; ++i)
		{
			int weight = balancer.next().getWeight();
			
			assert weight == 0 : weight;
		}
		
		balancer.add(new MockDatabase("1", 1));
		
		for (int i = 0; i < count; ++i)
		{
			int weight = balancer.next().getWeight();
			
			assert weight == 1 : weight;
		}
		
		balancer.add(new MockDatabase("2", 2));
		
		int[] expected = new int[] { 1, 2, 2 };
		
		for (int i = 0; i < count; ++i)
		{
			int weight = balancer.next().getWeight();
			
			assert expected[i % 3] == weight : weight;
		}
	}
}
