package net.sf.hajdbc.sql;

import java.sql.SQLException;

import javax.sql.DataSource;

public class DataSourceDatabaseBuilder extends CommonDataSourceDatabaseBuilder<DataSource, DataSourceDatabase>
{
	public DataSourceDatabaseBuilder(String id)
	{
		super(id, DataSource.class);
	}

	@Override
	public DataSourceDatabase build() throws SQLException
	{
		return new DataSourceDatabase(this.id, this.getDataSource(), this.credentials, this.weight, this.local);
	}
}
