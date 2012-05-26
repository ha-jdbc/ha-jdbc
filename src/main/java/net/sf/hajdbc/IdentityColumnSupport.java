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
package net.sf.hajdbc;

import java.sql.SQLException;


/**
 * @author Paul Ferraro
 */
public interface IdentityColumnSupport
{
	/**
	 * Parses a table name from the specified INSERT SQL statement that may contain identity columns.
	 * @param sql a SQL statement
	 * @return the name of a table, or null if this SQL statement is not an INSERT statement or this dialect does not support identity columns
	 * @throws SQLException
	 * @since 2.0
	 */
	String parseInsertTable(String sql) throws SQLException;
	
	/**
	 * Returns a SQL statement used reset the current value of an identity column.
	 * @param table a sequence name
	 * @param column a sequence name
	 * @param value a sequence value
	 * @return a SQL statement
	 * @throws SQLException 
	 * @since 2.0.2
	 */
	String getAlterIdentityColumnSQL(TableProperties table, ColumnProperties column, long value) throws SQLException;
}
