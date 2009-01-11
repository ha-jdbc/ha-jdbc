/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
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
	private final String name;
	
	protected AbstractTableProperties(DatabaseMetaDataSupport support, QualifiedName table)
	{
		this.name = support.qualifyNameForDML(table);
	}
	
	/**
	 * @see net.sf.hajdbc.TableProperties#getName()
	 */
	@Override
	public final String getName()
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
			
		String name = ((TableProperties) object).getName();
		
		return (name != null) && name.equals(this.name);
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
		return this.name;
	}
}
