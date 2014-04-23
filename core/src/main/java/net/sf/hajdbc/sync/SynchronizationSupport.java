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
package net.sf.hajdbc.sync;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Paul Ferraro
 *
 */
public interface SynchronizationSupport
{
	/**
	 * Drop all foreign key constraints on the target database
	 * @throws SQLException if database error occurs
	 */
	void dropForeignKeys() throws SQLException;
	
	/**
	 * Restores all foreign key constraints on the target database
	 * @throws SQLException if database error occurs
	 */
	void restoreForeignKeys() throws SQLException;
	
	/**
	 * Synchronizes the sequences on the target database with the source database.
	 * @throws SQLException if database error occurs
	 */
	void synchronizeSequences() throws SQLException;
	
	/**
	 * @throws SQLException
	 */
	void synchronizeIdentityColumns() throws SQLException;

	/**
	 * @throws SQLException
	 */
	void dropUniqueConstraints() throws SQLException;
	
	/**
	 * @throws SQLException
	 */
	void restoreUniqueConstraints() throws SQLException;
	
	/**
	 * Helper method for {@link java.sql.ResultSet#getObject(int)} with special handling for large objects.
	 * @param resultSet
	 * @param index
	 * @param type
	 * @return the object of the specified type at the specified index from the specified result set
	 * @throws SQLException
	 */
	Object getObject(ResultSet resultSet, int index, int type) throws SQLException;
	
	void rollback(Connection connection);
}
