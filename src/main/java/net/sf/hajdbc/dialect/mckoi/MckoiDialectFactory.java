package net.sf.hajdbc.dialect.mckoi;

import net.sf.hajdbc.dialect.Dialect;
import net.sf.hajdbc.dialect.DialectFactory;

public class MckoiDialectFactory implements DialectFactory
{
	private static final long serialVersionUID = -3028730694756617177L;

	@Override
	public String getId()
	{
		return "mckoi";
	}

	@Override
	public Dialect createDialect()
	{
		return new MckoiDialect();
	}
}
