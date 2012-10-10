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
package net.sf.hajdbc.state.bdb;

import java.io.File;

import net.sf.hajdbc.pool.CloseablePoolProvider;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

/**
 * @author paul
 */
public class BDBStateManager extends CloseablePoolProvider<Environment, DatabaseException>
{
	private EnvironmentConfig config = new EnvironmentConfig();
	
	public BDBStateManager()
	{
		super(Environment.class, DatabaseException.class);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.pool.PoolProvider#create()
	 */
	@Override
	public Environment create() throws DatabaseException
	{
		return new Environment(new File(""), this.config);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.pool.PoolProvider#isValid(java.lang.Object)
	 */
	@Override
	public boolean isValid(Environment environment)
	{
		try
		{
			environment.checkHandleIsValid();
			
			return true;
		}
		catch (DatabaseException e)
		{
			return false;
		}
	}
}
