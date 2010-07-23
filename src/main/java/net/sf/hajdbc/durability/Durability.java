/*
 * HA-JDBC: High-Availability JDBC
 * Copyright 2004-2009 Paul Ferraro
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
package net.sf.hajdbc.durability;

import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.ExceptionFactory;
import net.sf.hajdbc.sql.InvocationStrategy;
import net.sf.hajdbc.sql.Invoker;

/**
 * @author Paul Ferraro
 */
public interface Durability<Z, D extends Database<Z>>
{
	enum Phase
	{
		PREPARE, COMMIT, ROLLBACK, FORGET;
	}
	
	InvocationStrategy getInvocationStrategy(InvocationStrategy strategy, Phase phase, Object transactionId);
	
	<T, R, E extends Exception> Invoker<Z, D, T, R, E> getInvoker(Invoker<Z, D, T, R, E> invoker, Phase phase, Object transactionId, ExceptionFactory<E> exceptionFactory);
	
	void recover(Map<InvocationEvent, Map<String, InvokerEvent>> invokers);
}
