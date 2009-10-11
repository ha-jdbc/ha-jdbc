/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2008 Paul Ferraro
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
package net.sf.hajdbc.sql;

import java.sql.SQLException;

/**
 * @author Paul Ferraro
 * @param <D> the data source class
 */
public interface CommonDataSourceFactory<Z extends javax.sql.CommonDataSource>
{
	/**
	 * Creates a data source proxy to the specified cluster, using the configuration file at the specified location.
	 * @param id a database cluster identifier
	 * @param config the location of the configuration file for this cluster
	 * @return a proxied data source
	 * @throws SQLException if the data source proxy could not be created
	 */
	public Z createProxy(String id, String config) throws SQLException;
}