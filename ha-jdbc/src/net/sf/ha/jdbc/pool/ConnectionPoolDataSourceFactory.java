package net.sf.ha.jdbc.pool;

import net.sf.ha.jdbc.AbstractDataSourceFactory;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class ConnectionPoolDataSourceFactory extends AbstractDataSourceFactory
{
	/**
	 * @see net.sf.ha.jdbc.AbstractObjectFactory#getObjectClass()
	 */
	protected Class getObjectClass()
	{
		return ConnectionPoolDataSourceProxy.class;
	}
}
