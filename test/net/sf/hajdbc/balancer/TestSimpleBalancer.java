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

import net.sf.hajdbc.MockDatabase;

import org.testng.annotations.Test;

/**
 * @author  Paul Ferraro
 */
@Test
@SuppressWarnings("nls")
public class TestSimpleBalancer extends AbstractTestBalancer
{
	public TestSimpleBalancer()
	{
		super(new SimpleBalancer<Void>());
	}

	/**
	 * @see net.sf.hajdbc.balancer.AbstractTestBalancer#testNext()
	 */
	@Override
	public void testNext()
	{
		this.add(new MockDatabase("0", 0));
		
		String[] results = new String[20];
		
		for (int i = 0; i < results.length; ++i)
		{
			results[i] = this.next().getId();
		}

		String[] expected = new String[results.length];
		
		Arrays.fill(expected, "0");

		assert Arrays.equals(results, expected) : Arrays.asList(results);

		this.add(new MockDatabase("2", 2));
		
		for (int i = 0; i < results.length; ++i)
		{
			results[i] = this.next().getId();
		}
		
		Arrays.fill(expected, "2");
		
		assert Arrays.equals(results, expected) : Arrays.asList(results);

		this.add(new MockDatabase("1", 1));
		
		for (int i = 0; i < results.length; ++i)
		{
			results[i] = this.next().getId();
		}

		assert Arrays.equals(results, expected) : Arrays.asList(results);
	}
}
