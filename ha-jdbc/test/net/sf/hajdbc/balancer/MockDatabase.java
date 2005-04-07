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

import java.sql.Connection;

import net.sf.hajdbc.Database;

/**
 * @author  Paul Ferraro
 * @since   3.1
 */
public class MockDatabase implements Database
{
	private String id;
	private Integer weight;
	
	public MockDatabase(String id, int weight)
	{
		this.id = id;
		this.weight = new Integer(weight);
	}
	
	/**
	 * @see net.sf.hajdbc.Database#getId()
	 */
	public String getId()
	{
		return this.id;
	}

	/**
	 * @see net.sf.hajdbc.Database#connect(java.lang.Object)
	 */
	public Connection connect(Object connectionFactory)
	{
		return null;
	}

	/**
	 * @see net.sf.hajdbc.Database#createConnectionFactory()
	 */
	public Object createConnectionFactory()
	{
		return null;
	}

	/**
	 * @see net.sf.hajdbc.Database#getWeight()
	 */
	public Integer getWeight()
	{
		return this.weight;
	}
	
	public boolean equals(Object object)
	{
		Database database = (Database) object;
		
		return this.id.equals(database.getId());
	}
}
