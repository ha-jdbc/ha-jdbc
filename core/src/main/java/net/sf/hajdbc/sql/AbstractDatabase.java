/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
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
package net.sf.hajdbc.sql;

import net.sf.hajdbc.Credentials;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.Locality;
import net.sf.hajdbc.management.Description;
import net.sf.hajdbc.management.ManagedAttribute;

/**
 * @author  Paul Ferraro
 * @param <Z>
 */
public abstract class AbstractDatabase<Z> implements Database<Z>
{
	private final String id;
	private final Z connectionSource;
	private final Credentials credentials;
	private final Locality locality;
	private volatile int weight;

	protected AbstractDatabase(String id, Z connectionSource, Credentials credentials, int weight, Locality locality)
	{
		this.id = id;
		this.connectionSource = connectionSource;
		this.credentials = credentials;
		this.weight = weight;
		this.locality = locality;
	}
	
	@ManagedAttribute
	@Description("Uniquely identifies this database in the cluster")
	@Override
	public String getId()
	{
		return this.id;
	}

	@Override
	public Z getConnectionSource()
	{
		return this.connectionSource;
	}

	@Override
	public Credentials getCredentials()
	{
		return this.credentials;
	}

	@ManagedAttribute
	@Description("")
	@Override
	public int getWeight()
	{
		return this.weight;
	}

	@ManagedAttribute
	@Description("")
	@Override
	public void setWeight(int weight)
	{
		this.weight = weight;
	}

	@ManagedAttribute
	@Description("")
	@Override
	public Locality getLocality()
	{
		return this.locality;
	}

	@Override
	public int hashCode()
	{
		return this.id.hashCode();
	}

	@Override
	public boolean equals(Object object)
	{
		if ((object == null) || !(object instanceof Database<?>)) return false;
		
		String id = ((Database<?>) object).getId();
		
		return (id != null) && id.equals(this.id);
	}

	@Override
	public String toString()
	{
		return this.id;
	}

	@Override
	public int compareTo(Database<Z> database)
	{
		return this.id.compareTo(database.getId());
	}
}
