package net.sf.ha.jdbc.pool.xa;

import net.sf.ha.jdbc.AbstractObjectFactory;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class XADataSourceFactory extends AbstractObjectFactory
{
	/**
	 * @see net.sf.ha.jdbc.AbstractObjectFactory#getObjectClass()
	 */
	protected Class getObjectClass()
	{
		return XADataSourceProxy.class;
	}
}
