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
package net.sf.hajdbc.sql;

import java.io.Serializable;
import java.sql.SQLException;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseClusterConfigurationFactory;

/**
 * @author Paul Ferraro
 * @param <D> the data source class
 */
public interface CommonDataSourceFactory<Z extends javax.sql.CommonDataSource, D extends Database<Z>> extends Serializable
{
	/**
	 * Creates a data source proxy to the specified cluster, using the configuration file at the specified location.
	 * @param id a database cluster identifier
	 * @param config the location of the configuration file for this cluster
	 * @return a proxied data source
	 * @throws SQLException if the data source proxy could not be created
	 */
	Z createProxy(String id, DatabaseClusterConfigurationFactory<Z, D> factory) throws SQLException;
}
