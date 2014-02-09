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
package net.sf.hajdbc.codec;

/**
 * Abstract codec implementation that is its own factory
 * @author Paul Ferraro
 */
public abstract class AbstractCodec implements Codec, CodecFactory
{
	private static final long serialVersionUID = 848903379915175047L;

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.codec.CodecFactory#createCodec(java.lang.String)
	 */
	@Override
	public Codec createCodec(String clusterId)
	{
		return this;
	}
}
