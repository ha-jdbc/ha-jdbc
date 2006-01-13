/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
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
package net.sf.hajdbc;

import java.sql.SQLException;

/**
 * General interface for defining operation on an SQL object.
 * 
 * @author  Paul Ferraro
 * @version $Revision$
 * @param <S>
 * @param <T> 
 * @since   1.0
 */
public interface Operation<T, S>
{
	/**
	 * Executes this operation of the specified SQL object for the specified database.
	 * @param database a database descriptor
	 * @param sqlObject a java.sql or javax.sql object.
	 * @return the result of this operation
	 * @throws SQLException if execution fails
	 */
	public S execute(Database database, T sqlObject) throws SQLException;
}
