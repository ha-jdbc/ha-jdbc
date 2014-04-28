/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2014  Paul Ferraro
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

import net.sf.hajdbc.configuration.PropertiesBuilder;

public interface DatabaseBuilder<Z, D extends Database<Z>> extends PropertiesBuilder<D>
{
	int ID_MAX_SIZE = 64;

	DatabaseBuilder<Z, D> connectionSource(Z connectionSource);

	DatabaseBuilder<Z, D> location(String location);

	DatabaseBuilder<Z, D> credentials(String user, String password);

	DatabaseBuilder<Z, D> weight(int weight);

	DatabaseBuilder<Z, D> locality(Locality locality);
}
