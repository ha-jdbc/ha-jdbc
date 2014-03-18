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
							editor.setAsText(value);
							descriptor.getWriteMethod().invoke(dataSource, editor.getValue());
						}
					}
				}
			}
			catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IntrospectionException | InvocationTargetException e)
			{
				throw new SQLException(e);
			}
		}
		return dataSource;
	}
}
