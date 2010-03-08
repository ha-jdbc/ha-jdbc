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
package net.sf.hajdbc.state.jdbm;

import java.io.IOException;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import net.sf.hajdbc.pool.AbstractPoolProvider;

/**
 * @author paul
 *
 */
public class JDBMStateManager extends AbstractPoolProvider<RecordManager, IOException>
{
	public JDBMStateManager()
	{
		super(RecordManager.class, IOException.class);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.pool.PoolProvider#close(java.lang.Object)
	 */
	@Override
	public void close(RecordManager manager)
	{
		try
		{
			manager.close();
		}
		catch (IOException e)
		{
			// log
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.pool.PoolProvider#create()
	 */
	@Override
	public RecordManager create() throws IOException
	{
		return RecordManagerFactory.createRecordManager("");
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.pool.PoolProvider#isValid(java.lang.Object)
	 */
	@Override
	public boolean isValid(RecordManager manager)
	{
		return true;
	}
}
