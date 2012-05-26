/*
 * HA-JDBC: High-Availablity JDBC
 * Copyright 2011 Paul Ferraro
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

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.util.Strings;

/**
 * @author Paul Ferraro
 */
public class QualifiedNameImpl implements QualifiedName
{
	private final String schema;
	private final String name;
	private final String ddlName;
	private final String dmlName;

	public QualifiedNameImpl(String name)
	{
		this.schema = null;
		this.name = name;
		this.ddlName = name;
		this.dmlName = name;
	}

	public QualifiedNameImpl(String schema, String name, boolean supportsSchemasInDDL, boolean supportsSchemasInDML)
	{
		this.schema = schema;
		this.name = name;
		this.ddlName = this.qualify(supportsSchemasInDDL);
		this.dmlName = (supportsSchemasInDDL == supportsSchemasInDML) ? this.ddlName : this.qualify(supportsSchemasInDML);
	}

	public QualifiedNameImpl(String schema, String name, DatabaseMetaData metaData) throws SQLException
	{
		this(schema, name, metaData.supportsSchemasInTableDefinitions(), metaData.supportsSchemasInDataManipulation());
	}

	private String qualify(boolean supportsSchemas)
	{
		if (supportsSchemas && (this.schema != null))
		{
			return new StringBuilder(this.schema).append(Strings.DOT).append(this.name).toString();
		}
		
		return this.name;
	}
	
	@Override
	public String getSchema()
	{
		return this.schema;
	}

	@Override
	public String getName()
	{
		return this.name;
	}

	@Override
	public String getDDLName()
	{
		return this.ddlName;
	}

	@Override
	public String getDMLName()
	{
		return this.dmlName;
	}
	
	@Override
	public boolean equals(Object object)
	{
		if ((object == null) || !(object instanceof QualifiedName)) return false;
		
		QualifiedName qn = (QualifiedName) object;
		
		return ((this.schema == qn.getSchema()) || ((this.schema != null) && (qn.getSchema() != null) && this.schema.equals(qn.getSchema()))) && this.name.equals(qn.getName());
	}

	@Override
	public int hashCode()
	{
		return this.name.hashCode();
	}

	@Override
	public String toString()
	{
		return this.ddlName;
	}
}
