package net.sf.hajdbc.dialect;


public class StandardDialectFactory implements DialectFactory
{
	private static final long serialVersionUID = -2493078684331580988L;

	@Override
	public String getId()
	{
		return "standard";
	}

	@Override
	public Dialect createDialect()
	{
		return new StandardDialect();
	}
}
