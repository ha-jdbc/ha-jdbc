/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2013  Paul Ferraro
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc.sql;

import java.util.SortedMap;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.util.Objects;

/**
 * @author Paul Ferraro
 */
public class SimpleInvocationResultFactory<Z, D extends Database<Z>, R> implements InvocationResultFactory<Z, D, R>
{
	@Override
	public boolean differs(R primaryResult, R backupResult)
	{
		return !Objects.equals(primaryResult, backupResult);
	}
	
	@Override
	public R createResult(SortedMap<D, R> results)
	{
		return results.values().iterator().next();
	}
}
