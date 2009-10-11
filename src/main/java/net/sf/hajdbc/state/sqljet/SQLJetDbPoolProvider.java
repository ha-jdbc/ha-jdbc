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
package net.sf.hajdbc.state.sqljet;

import java.io.File;

import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.table.SqlJetDb;

import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;
import net.sf.hajdbc.pool.PoolProvider;

/**
 * <a href="http://sqljet.com/">SQLJet</a> is a java port of <a href="http://www.sqlite.org/">SQLite</a>.
 * 
 * @author paul
 */
public class SQLJetDbPoolProvider implements PoolProvider<SqlJetDb, SqlJetException>
{
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.pool.PoolProvider#close(java.lang.Object)
	 */
	@Override
	public void close(SqlJetDb database)
	{
		try
		{
			database.close();
		}
		catch (SqlJetException e)
		{
			this.logger.log(Level.WARN, e, e.getMessage());
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.pool.PoolProvider#create()
	 */
	@Override
	public SqlJetDb create() throws SqlJetException
	{
		return SqlJetDb.open(new File(""), true);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.pool.PoolProvider#isValid(java.lang.Object)
	 */
	@Override
	public boolean isValid(SqlJetDb database)
	{
		return database.isOpen();
	}
}
