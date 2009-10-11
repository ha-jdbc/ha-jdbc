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
package net.sf.hajdbc.cache;

import java.sql.SQLException;
import java.util.Collection;


/**
 * @author Paul Ferraro
 *
 */
public interface TableProperties
{
	public String getName();
	
	public Collection<String> getColumns() throws SQLException;
	
	public ColumnProperties getColumnProperties(String column) throws SQLException;
	
	public UniqueConstraint getPrimaryKey() throws SQLException;
	
	public Collection<ForeignKeyConstraint> getForeignKeyConstraints() throws SQLException;
	
	public Collection<UniqueConstraint> getUniqueConstraints() throws SQLException;
	
	public Collection<String> getIdentityColumns() throws SQLException;
}
