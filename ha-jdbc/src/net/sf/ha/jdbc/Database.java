package net.sf.ha.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public interface Database
{
	public Connection connect(Object object) throws SQLException;
	
	public String getId();
}
