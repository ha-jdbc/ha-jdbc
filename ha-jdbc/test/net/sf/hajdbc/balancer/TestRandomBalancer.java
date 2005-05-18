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

import java.util.Arrays;

import net.sf.hajdbc.Balancer;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public class TestRandomBalancer extends TestBalancer
{
	protected Balancer createBalancer()
	{
		return new RandomBalancer();
	}

	protected void testNext(Balancer balancer)
	{
		int count = 100;
		int[] results = new int[3];
		
		Arrays.fill(results, 0);
		
		balancer.add(new MockDatabase("0", 0));
		
		for (int i = 0; i < count; ++i)
		{
			results[balancer.next().getWeight().intValue()] += 1;
		}

		assert results[0] == count : "Expected 100, actual " + results[0];
		assert results[1] == 0 : "Expected 0, actual " + results[1];
		assert results[2] == 0 : "Expected 0, actual " + results[2];

		balancer.add(new MockDatabase("1", 1));
		
		Arrays.fill(results, 0);
		
		for (int i = 0; i < count; ++i)
		{
			results[balancer.next().getWeight().intValue()] += 1;
		}
		
		assert results[0] == 0 : "Expected 0, actual " + results[0];
		assert results[1] == count : "Expected 100, actual " + results[1];
		assert results[2] == 0 : "Expected 0, actual " + results[2];

		balancer.add(new MockDatabase("2", 2));

		Arrays.fill(results, 0);
		
		for (int i = 0; i < count; ++i)
		{
			results[balancer.next().getWeight().intValue()] += 1;
		}

		assert results[0] == 0 : "Expected 0, actual " + results[0];
		assert results[1] < 50 : "Expected <50, actual " + results[1];
		assert results[2] > 50 : "Expected >50, actual " + results[2];
	}
}
