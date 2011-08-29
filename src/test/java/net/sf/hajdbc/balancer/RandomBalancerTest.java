/*
 * HA-JDBC: High-Availablity JDBC
 * Copyright 2010 Paul Ferraro
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc.balancer;

import net.sf.hajdbc.MockDatabase;

import static org.junit.Assert.*;


/**
 * @author Paul Ferraro
 */
public class RandomBalancerTest extends AbstractBalancerTest
{
	public RandomBalancerTest()
	{
		super(BalancerFactoryEnum.RANDOM);
	}
	
	@Override
	public void next(Balancer<Void, MockDatabase> balancer)
	{
		int count = 1000;
		int[] results = new int[] { 0, 0, 0 };
		
		for (int i = 0; i < count; ++i)
		{
			results[balancer.next().getWeight()] += 1;
		}

		assertEquals(0, results[0]);
		assertTrue(Integer.toString(results[1]), results[1] < (count / 2));
		assertTrue(Integer.toString(results[2]), results[2] > (count / 2));
	}
}
