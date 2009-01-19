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
package net.sf.hajdbc.cache;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import net.sf.hajdbc.Dialect;


/**
 * @author Paul Ferraro
 *
 */
public class DatabaseMetaDataSupportFactoryImpl implements DatabaseMetaDataSupportFactory
{
	/**
	 * @see net.sf.hajdbc.cache.DatabaseMetaDataSupportFactory#createSupport(java.sql.DatabaseMetaData, net.sf.hajdbc.Dialect)
	 */
	@Override
	public DatabaseMetaDataSupport createSupport(DatabaseMetaData metaData, Dialect dialect) throws SQLException
	{
		return new DatabaseMetaDataSupportImpl(metaData, dialect);
	}
}
