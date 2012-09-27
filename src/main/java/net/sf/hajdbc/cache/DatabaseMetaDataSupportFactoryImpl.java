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
package net.sf.hajdbc.cache;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import net.sf.hajdbc.dialect.Dialect;


/**
 * @author Paul Ferraro
 *
 */
public class DatabaseMetaDataSupportFactoryImpl implements DatabaseMetaDataSupportFactory
{
	private static final long serialVersionUID = -2989879373607702506L;

	/**
	 * @see net.sf.hajdbc.cache.DatabaseMetaDataSupportFactory#createSupport(java.sql.DatabaseMetaData, net.sf.hajdbc.dialect.Dialect)
	 */
	@Override
	public DatabaseMetaDataSupport createSupport(DatabaseMetaData metaData, Dialect dialect) throws SQLException
	{
		return new DatabaseMetaDataSupportImpl(metaData, dialect);
	}
}
