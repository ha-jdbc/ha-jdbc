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

import java.util.Arrays;

import org.testng.annotations.Test;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.MockDatabase;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
@Test
@SuppressWarnings("nls")
public class TestSimpleBalancer extends AbstractTestBalancer
{
	@Override
	protected Balancer<Void> createBalancer()
	{
		return new SimpleBalancer<Void>();
	}

	@Override
	protected void next(Balancer<Void> balancer)
	{
		balancer.add(new MockDatabase("0", 0));
		
		String[] results = new String[20];
		
		for (int i = 0; i < results.length; ++i)
		{
			results[i] = balancer.next().getId();
		}

		String[] expected = new String[results.length];
		
		Arrays.fill(expected, "0");

		assert Arrays.equals(results, expected) : Arrays.asList(results);

		balancer.add(new MockDatabase("2", 2));
		
		for (int i = 0; i < results.length; ++i)
		{
			results[i] = balancer.next().getId();
		}
		
		Arrays.fill(expected, "2");
		
		assert Arrays.equals(results, expected) : Arrays.asList(results);

		balancer.add(new MockDatabase("1", 1));
		
		for (int i = 0; i < results.length; ++i)
		{
			results[i] = balancer.next().getId();
		}

		assert Arrays.equals(results, expected) : Arrays.asList(results);
	}
}
