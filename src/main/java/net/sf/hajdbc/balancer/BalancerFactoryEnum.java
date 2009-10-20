/*
 * HA-JDBC: High-Availability JDBC
 * Copyright 2004-2009 Paul Ferraro
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

import java.util.Set;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;
import javax.xml.bind.annotation.XmlType;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.balancer.load.LoadBalancerFactory;
import net.sf.hajdbc.balancer.random.RandomBalancerFactory;
import net.sf.hajdbc.balancer.roundrobin.RoundRobinBalancerFactory;
import net.sf.hajdbc.balancer.simple.SimpleBalancerFactory;

/**
 * Enumerated balancing implementations.
 * @author  Paul Ferraro
 */
@XmlEnum(String.class)
@XmlType(name="balancer")
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
