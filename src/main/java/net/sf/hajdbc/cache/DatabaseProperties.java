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
public interface DatabaseProperties
{
	public Collection<TableProperties> getTables() throws SQLException;

	public boolean supportsSelectForUpdate() throws SQLException;
	
	public boolean locatorsUpdateCopy() throws SQLException;
	
	public TableProperties findTable(String table) throws SQLException;
	
	public Collection<SequenceProperties> getSequences() throws SQLException;
	
	public SequenceProperties findSequence(String sequence) throws SQLException;
}
