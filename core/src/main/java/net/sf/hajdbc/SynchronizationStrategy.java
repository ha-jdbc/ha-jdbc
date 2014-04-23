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

import java.io.Serializable;
import java.sql.SQLException;

import net.sf.hajdbc.sync.SynchronizationContext;


/**
 * @author  Paul Ferraro
 */
public interface SynchronizationStrategy extends Identifiable, Serializable
{
	<Z, D extends Database<Z>> void init(DatabaseCluster<Z, D> cluster);
	
	/**
	 * Synchronizes a target database with a source database as defined by the synchronization context.
	 * @param <Z>
	 * @param <D>
	 * @param context a synchronization context
	 * @throws SQLException if synchronization fails
	 */
	<Z, D extends Database<Z>> void synchronize(SynchronizationContext<Z, D> context) throws SQLException;
	
	<Z, D extends Database<Z>> void destroy(DatabaseCluster<Z, D> cluster);	
}
