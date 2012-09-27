package net.sf.hajdbc.dialect.postgresql;

import net.sf.hajdbc.dialect.Dialect;
import net.sf.hajdbc.dialect.DialectFactory;

public class PostgreSQLDialectFactory implements DialectFactory
{
	private static final long serialVersionUID = -1228325684874648061L;

	@Override
	public String getId()
	{
		return "postgresql";
	}

	@Override
	public Dialect createDialect()
	{
		return new PostgreSQLDialect();
	}
}
