package net.sf.ha.jdbc;



/**
 * @author  Paul Ferraro
 * @version $Revision$
 */
public class DataSourceFactory extends AbstractObjectFactory
{
	/**
	 * @see net.sf.ha.jdbc.AbstractObjectFactory#getObjectClass()
	 */
	protected Class getObjectClass()
	{
		return DataSourceProxy.class;
	}
}
