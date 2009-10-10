/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2009 Paul Ferraro
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
package net.sf.hajdbc.balancer.random;

import java.util.Set;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.balancer.Balancer;
import net.sf.hajdbc.balancer.BalancerFactory;

/**
 * @author paul
 *
 */
public class RandomBalancerFactory implements BalancerFactory
{
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.balancer.BalancerFactory#createBalancer(java.util.Set)
	 */
	@Override
	public <Z, D extends Database<Z>> Balancer<Z, D> createBalancer(Set<D> databaseSet)
	{
		return new RandomBalancer<Z, D>();
	}
}
