package net.sf.ha.jdbc;



/**
 * @author  Paul Ferraro
 * @version $Revision$
 */
public class DataSourceFactory extends AbstractDataSourceFactory
{
	/**
	 * @see net.sf.ha.jdbc.AbstractObjectFactory#getObjectClass()
	 */
	protected Class getObjectClass()
	{
		return DataSourceProxy.class;
	}
}
