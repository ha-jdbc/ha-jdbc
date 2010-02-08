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

import net.sf.hajdbc.util.Strings;

/**
 * Tuple that stores the schema name and object name of a schema qualified object.
 * @author Paul Ferraro
 */
public class QualifiedName
{
	private final String schema;
	private final String name;
	
	public QualifiedName(String schema, String name)
	{
		this.schema = schema;
		this.name = name;
	}
	
	public QualifiedName(String name)
	{
		this.schema = null;
		this.name = name;
	}
	
	public String getSchema()
	{
		return this.schema;
	}
	
	public String getName()
	{
		return this.name;
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object)
	{
		if ((object == null) || !(object instanceof QualifiedName))
		{
			return false;
		}
		
		QualifiedName qn = (QualifiedName) object;
		
		return ((this.schema == qn.schema) || ((this.schema != null) && (qn.schema != null) && this.schema.equals(qn.schema))) && this.name.equals(qn.name);
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		return this.name.hashCode();
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		StringBuilder builder = new StringBuilder();
		
		if (this.schema != null)
		{
			builder.append(this.schema).append(Strings.DOT);
		}
		
		return builder.append(this.name).toString();
	}
}
