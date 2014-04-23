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

import java.io.Serializable;
import java.sql.SQLException;

import net.sf.hajdbc.IdentifiableMatcher;
import net.sf.hajdbc.util.ServiceLoaders;

/**
 * Codec factory that whose decoding behavior is determined by a prefix.
 * @author Paul Ferraro
 */
public class MultiplexingDecoderFactory implements DecoderFactory, Serializable {

	public static final String DELIMITER = ":";
	private static final long serialVersionUID = 4413927326976263687L;

	@Override
	public Decoder createDecoder(String clusterId)
	{
		return new MultiplexingDecoder(clusterId);
	}

	private static class MultiplexingDecoder implements Decoder
	{
		private final String clusterId;
		
		MultiplexingDecoder(String clusterId)
		{
			this.clusterId = clusterId;
		}
		
		@Override
		public String decode(String value) throws SQLException
		{
			if (value == null) return null;
			int index = value.indexOf(DELIMITER);
			String id = (index >= 0) ? value.substring(0, index) : null;
			String source = (index >= 0) ? value.substring(index + 1) : value;
			CodecFactory factory = ServiceLoaders.findRequiredService(new IdentifiableMatcher<CodecFactory>(id), CodecFactory.class);
			return factory.createCodec(this.clusterId).decode(source);
		}
	}
}
