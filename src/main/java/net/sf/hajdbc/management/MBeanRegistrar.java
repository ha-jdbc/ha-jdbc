/*
 * HA-JDBC: High-Availablity JDBC
 * Copyright 2010 Paul Ferraro
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
package net.sf.hajdbc.management;

import javax.management.JMException;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;

/**
 * @author Paul Ferraro
 */
public interface MBeanRegistrar<Z, D extends Database<Z>>
{
	void register(DatabaseCluster<Z, D> cluster) throws JMException;
	void register(DatabaseCluster<Z, D> cluster, D database) throws JMException;
	
	void unregister(DatabaseCluster<Z, D> cluster);
	void unregister(DatabaseCluster<Z, D> cluster, D database);
}
