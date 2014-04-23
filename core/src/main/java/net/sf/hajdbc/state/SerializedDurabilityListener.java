/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
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
package net.sf.hajdbc.state;

/**
 * @author Paul Ferraro
 */
public interface SerializedDurabilityListener
{
	void beforeInvocation(byte[] transactionId, byte phase, byte exceptionType);
	
	void afterInvocation(byte[] transactionId, byte phase);
	
	void beforeInvoker(byte[] transactionId, byte phase, String databaseId);
	
	void afterInvoker(byte[] transactionId, byte phase, String databaseId, byte[] result);
}
