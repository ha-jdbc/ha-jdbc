package net.sf.hajdbc.dialect.oracle;

import net.sf.hajdbc.dialect.Dialect;
import net.sf.hajdbc.dialect.DialectFactory;

public class OracleDialectFactory implements DialectFactory
{
	private static final long serialVersionUID = -4359494272845628592L;

	@Override
	public String getId()
	{
		return "oracle";
	}

	@Override
	public Dialect createDialect()
	{
		return new OracleDialect();
	}
}
