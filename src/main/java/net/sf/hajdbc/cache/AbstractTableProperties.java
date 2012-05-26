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
import java.util.Map;

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.TableProperties;


/**
 * @author Paul Ferraro
 *
 */
public abstract class AbstractTableProperties implements TableProperties
{
	private final QualifiedName name;

	protected AbstractTableProperties(QualifiedName name) {
		this.name = name;
	}
	
	/**
	 * @see net.sf.hajdbc.TableProperties#getName()
	 */
	@Override
	public final QualifiedName getName()
	{
		return this.name;
	}

	/**
	 * @see net.sf.hajdbc.TableProperties#getColumns()
	 */
	@Override
	public final Collection<String> getColumns() throws SQLException
	{
		return this.getColumnMap().keySet();
	}

	/**
	 * @see net.sf.hajdbc.TableProperties#getColumnProperties(java.lang.String)
	 */
	@Override
	public final ColumnProperties getColumnProperties(String column) throws SQLException
	{
		return this.getColumnMap().get(column);
	}

	protected abstract Map<String, ColumnProperties> getColumnMap() throws SQLException;
	
	/**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public final boolean equals(Object object)
	{
		if ((object == null) || !(object instanceof TableProperties)) return false;
		
		return this.name.equals(((TableProperties) object).getName());
	}

	/**
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public final int hashCode()
	{
		return this.name.hashCode();
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public final String toString()
	{
		return this.name.getDDLName();
	}
}
