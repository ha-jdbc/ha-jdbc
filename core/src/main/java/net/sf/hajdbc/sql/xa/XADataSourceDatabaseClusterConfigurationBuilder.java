package net.sf.hajdbc.sql.xa;

import javax.sql.XADataSource;

import net.sf.hajdbc.DatabaseBuilderFactory;
import net.sf.hajdbc.DatabaseClusterConfigurationBuilder;

public class XADataSourceDatabaseClusterConfigurationBuilder extends DatabaseClusterConfigurationBuilder<XADataSource, XADataSourceDatabase, XADataSourceDatabaseBuilder>
{
	public XADataSourceDatabaseClusterConfigurationBuilder()
	{
		super(new DatabaseBuilderFactory<XADataSource, XADataSourceDatabase, XADataSourceDatabaseBuilder>()
		{
			@Override
			public XADataSourceDatabaseBuilder createBuilder(String id)
			{
				return new XADataSourceDatabaseBuilder(id);
			}
		});
	}
}
