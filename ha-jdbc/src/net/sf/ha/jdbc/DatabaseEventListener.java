package net.sf.ha.jdbc;

import java.util.EventListener;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public interface DatabaseEventListener extends EventListener
{
	public String getClusterName();
	
	public void deactivated(DatabaseEvent event);
}
