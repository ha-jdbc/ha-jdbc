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

/**
 * @author  Paul Ferraro
 * @since   1.1
 */
public enum BalancerFactory
{
	SIMPLE(SimpleBalancer.class),
	RANDOM(RandomBalancer.class),
	ROUND_ROBIN(RoundRobinBalancer.class),
	LOAD(LoadBalancer.class);

	private static final char UNDERSCORE = '_';
	private static final char DASH = '-';
	
	@SuppressWarnings("unchecked")
	private Class<? extends Balancer> balancerClass;
	
	@SuppressWarnings("unchecked")
	private BalancerFactory(Class<? extends Balancer> balancerClass)
	{
		this.balancerClass = balancerClass;
	}
	
	/**
	 * Creates a new instance of the Balancer implementation identified by the specified identifier
	 * @param id an enumerated balancer identifier
	 * @return a new Balancer instance
	 * @throws Exception if specified balancer identifier is invalid
	 */
	@SuppressWarnings("unchecked")
	public static Balancer deserialize(String id) throws Exception
	{
		try
		{
			return BalancerFactory.valueOf(id.toUpperCase().replace(DASH, UNDERSCORE)).balancerClass.newInstance();
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
	@SuppressWarnings("unchecked")
	public static String serialize(Balancer balancer)
	{
		Class<? extends Balancer> balancerClass = balancer.getClass();
		
		for (BalancerFactory factory: BalancerFactory.values())
		{
			if (balancerClass.equals(factory.balancerClass))
			{
				return factory.name().toLowerCase().replace(UNDERSCORE, DASH);
			}
		}
		
		throw new IllegalArgumentException(Messages.getMessage(Messages.INVALID_BALANCER, balancer.getClass()));
	}
}
