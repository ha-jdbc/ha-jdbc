package net.sf.ha.jdbc;

import java.sql.SQLException;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public interface Operation
{
	public Object execute(Database database, Object object) throws SQLException;
}
