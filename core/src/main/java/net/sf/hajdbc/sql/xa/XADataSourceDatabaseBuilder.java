package net.sf.hajdbc.sql.xa;

import java.sql.SQLException;

import javax.sql.XADataSource;

import net.sf.hajdbc.sql.CommonDataSourceDatabaseBuilder;

public class XADataSourceDatabaseBuilder extends CommonDataSourceDatabaseBuilder<XADataSource, XADataSourceDatabase>
{
	private volatile boolean force2PC = false;
	
	public XADataSourceDatabaseBuilder(String id)
	{
		super(id, XADataSource.class);
	}

	public XADataSourceDatabaseBuilder force2PC(boolean force2PC)
	{
		this.force2PC = force2PC;
		return this;
	}

	@Override
	public XADataSourceDatabase build() throws SQLException
	{
		return new XADataSourceDatabase(this.id, this.getDataSource(), this.credentials, this.weight, this.local, this.force2PC);
	}
}
