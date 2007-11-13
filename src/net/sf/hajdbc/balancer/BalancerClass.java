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
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.util.ClassEnum;
import net.sf.hajdbc.util.Strings;

/**
 * @author  Paul Ferraro
 * @since   1.1
 */
public enum BalancerClass implements ClassEnum<Balancer<?>>
{
	SIMPLE(SimpleBalancer.class),
	RANDOM(RandomBalancer.class),
	ROUND_ROBIN(RoundRobinBalancer.class),
	LOAD(LoadBalancer.class);

	@SuppressWarnings("unchecked")
	private Class<? extends Balancer> balancerClass;
	
	@SuppressWarnings("unchecked")
	private BalancerClass(Class<? extends Balancer> balancerClass)
	{
		this.balancerClass = balancerClass;
	}
	
	/**
	 * @see net.sf.hajdbc.util.ClassEnum#isInstance(java.lang.Object)
	 */
	@Override
	public boolean isInstance(Balancer<?> balancer)
	{
		return this.balancerClass.equals(balancer.getClass());
	}
	
	/**
	 * @see net.sf.hajdbc.util.ClassEnum#newInstance()
	 */
	@Override
	public Balancer<?> newInstance() throws Exception
	{
		return this.balancerClass.newInstance();
	}
	
	/**
	 * Creates a new instance of the Balancer implementation identified by the specified identifier
	 * @param id an enumerated balancer identifier
	 * @return a new Balancer instance
	 * @throws Exception if specified balancer identifier is invalid
	 */
	public static Balancer<?> deserialize(String id) throws Exception
	{
		try
		{
			return BalancerClass.valueOf(id.toUpperCase().replace(Strings.DASH, Strings.UNDERSCORE)).newInstance();
		}
		catch (IllegalArgumentException e)
		{
			throw new IllegalArgumentException(Messages.getMessage(Messages.INVALID_BALANCER, id));
		}
	}
	
	/**
	 * Return the identifier of the specified Balancer.
	 * @param balancer a Balancer implementation
	 * @return the class name of this balancer
	 */
	public static String serialize(Balancer<?> balancer)
	{
		for (BalancerClass balancerClass: BalancerClass.values())
		{
			if (balancerClass.isInstance(balancer))
			{
				return balancerClass.name().toLowerCase().replace(Strings.UNDERSCORE, Strings.DASH);
			}
		}
		
		throw new IllegalArgumentException(Messages.getMessage(Messages.INVALID_BALANCER, balancer.getClass()));
	}
}
