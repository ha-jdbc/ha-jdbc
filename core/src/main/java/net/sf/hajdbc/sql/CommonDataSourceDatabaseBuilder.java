/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2014  Paul Ferraro
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
package net.sf.hajdbc.sql;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.beans.PropertyEditor;
import java.beans.PropertyEditorManager;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.CommonDataSource;

import net.sf.hajdbc.Database;

public abstract class CommonDataSourceDatabaseBuilder<Z extends CommonDataSource, D extends Database<Z>> extends AbstractDatabaseBuilder<Z, D>
{
	private final Class<Z> dataSourceClass;

	public CommonDataSourceDatabaseBuilder(String id, Class<Z> dataSourceClass)
	{
		super(id);
		this.dataSourceClass = dataSourceClass;
	}

	public CommonDataSourceDatabaseBuilder<Z, D> dataSource(Z dataSource)
	{
		return this.connectionSource(dataSource);
	}

	@Override
	public CommonDataSourceDatabaseBuilder<Z, D> connectionSource(Z connectionSource)
	{
		super.connectionSource(connectionSource);
		return this;
	}

	/**
	 * Alias for {@link #connectionSource(CommonDataSource)}.
	 * @param className
	 * @return
	 */
	public CommonDataSourceDatabaseBuilder<Z, D> dataSourceClass(String className)
	{
		return this.location(className);
	}

	@Override
	public CommonDataSourceDatabaseBuilder<Z, D> location(String location)
	{
		super.location(location);
		return this;
	}

	@Override
	public CommonDataSourceDatabaseBuilder<Z, D> property(String name, String value)
	{
		super.property(name, value);
		return this;
	}

	@Override
	public CommonDataSourceDatabaseBuilder<Z, D> credentials(String user, String password)
	{
		super.credentials(user, password);
		return this;
	}

	@Override
	public CommonDataSourceDatabaseBuilder<Z, D> weight(int weight)
	{
		super.weight(weight);
		return this;
	}

	@Override
	public CommonDataSourceDatabaseBuilder<Z, D> local(boolean local)
	{
		super.local(local);
		return this;
	}

	@Override
	public CommonDataSourceDatabaseBuilder<Z, D> read(D database)
	{
		super.read(database);
		return this;
	}

	protected Z getDataSource() throws SQLException
	{
		Z dataSource = this.connectionSource;
		if (dataSource == null)
		{
			String location = this.location;
			if (location == null)
			{
				throw new SQLException("No data source specified");
			}
			try
			{
				Class<? extends Z> dataSourceClass = this.getClass().getClassLoader().loadClass(location).asSubclass(this.dataSourceClass);
				dataSource = dataSourceClass.newInstance();
				
				Properties properties = this.properties;
				if (!properties.isEmpty())
				{
					for (PropertyDescriptor descriptor: Introspector.getBeanInfo(dataSourceClass).getPropertyDescriptors())
					{
						String value = properties.getProperty(descriptor.getName());
						
						if (value != null)
						{
							PropertyEditor editor = PropertyEditorManager.findEditor(descriptor.getPropertyType());
							if (editor != null)
							{
								editor.setAsText(value);
								descriptor.getWriteMethod().invoke(dataSource, editor.getValue());
							}
						}
					}
				}
			}
			catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IntrospectionException | IllegalArgumentException | InvocationTargetException e)
			{
				throw new SQLException(e);
			}
		}
		return dataSource;
	}
}
