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

import java.util.Set;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.balancer.load.LoadBalancerFactory;
import net.sf.hajdbc.balancer.random.RandomBalancerFactory;
import net.sf.hajdbc.balancer.roundrobin.RoundRobinBalancerFactory;
import net.sf.hajdbc.balancer.simple.SimpleBalancerFactory;

/**
 * @author  Paul Ferraro
 * @since   1.1
 */
@XmlEnum(String.class)
public enum BalancerFactoryEnum implements BalancerFactory
{
	@XmlEnumValue("simple")
	SIMPLE(new SimpleBalancerFactory()),
	@XmlEnumValue("random")
	RANDOM(new RandomBalancerFactory()),
	@XmlEnumValue("round-robin")
	ROUND_ROBIN(new RoundRobinBalancerFactory()),
	@XmlEnumValue("load")
	LOAD(new LoadBalancerFactory());

	private final BalancerFactory factory;
	
	private BalancerFactoryEnum(BalancerFactory factory)
	{
		this.factory = factory;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.balancer.BalancerFactory#createBalancer(java.util.Set)
	 */
	@Override
	public <Z, D extends Database<Z>> Balancer<Z, D> createBalancer(Set<D> databaseSet)
	{
		return this.factory.createBalancer(databaseSet);
	}
}
