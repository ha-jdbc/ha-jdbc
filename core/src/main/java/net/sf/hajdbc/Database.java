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

/**
 * @author  Paul Ferraro
 * @param <Z> connection source (e.g. Driver, DataSource, etc.)
 */
public interface Database<Z> extends Comparable<Database<Z>>
{
	String getId();

	Z getConnectionSource();

	Credentials getCredentials();
	
	int getWeight();
	void setWeight(int weight);

	Locality getLocality();

	Connection connect(Decoder decoder) throws SQLException;

	String getLocation();

	Properties getProperties();
}
