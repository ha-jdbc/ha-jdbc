package net.sf.hajdbc.dialect.mysql;

import net.sf.hajdbc.dialect.Dialect;
import net.sf.hajdbc.dialect.DialectFactory;

public class MySQLDialectFactory implements DialectFactory
{
	private static final long serialVersionUID = 805698351955749293L;

	@Override
	public String getId()
	{
		return "mysql";
	}

	@Override
	public Dialect createDialect()
	{
		return new MySQLDialect();
	}
}
