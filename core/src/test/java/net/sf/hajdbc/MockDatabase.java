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
package net.sf.hajdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import net.sf.hajdbc.codec.Decoder;
import net.sf.hajdbc.sql.AbstractDatabase;

/**
 * @author Paul Ferraro
 *
 */
public class MockDatabase extends AbstractDatabase<Void>
{
	public MockDatabase()
	{
		this("");
	}
	
	public MockDatabase(String id)
	{
		this(id, 1);
	}
	
	public MockDatabase(String id, int weight)
	{
		super(id, null, null, weight, Locality.REMOTE);
	}

	@Override
	public Connection connect(Decoder decoder) throws SQLException
	{
		return null;
	}

	@Override
	public Void getConnectionSource()
	{
		return null;
	}

	@Override
	public int hashCode()
	{
		try
		{
			return Integer.parseInt(this.getId());
		}
		catch (NumberFormatException e)
		{
			return super.hashCode();
		}
	}

	@Override
	public String getLocation()
	{
		return null;
	}

	@Override
	public Properties getProperties()
	{
		return null;
	}
}
