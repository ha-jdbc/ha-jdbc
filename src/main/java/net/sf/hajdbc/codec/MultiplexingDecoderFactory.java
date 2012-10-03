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
	public Decoder createDecoder(String clusterId) throws SQLException
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
			int index = value.indexOf(DELIMITER);
			String id = (index >= 0) ? value.substring(0, index) : null;
			String source = (index >= 0) ? value.substring(index + 1) : value;
			CodecFactory factory = ServiceLoaders.findRequiredService(new IdentifiableMatcher<CodecFactory>(id), CodecFactory.class);
			return factory.createCodec(this.clusterId).decode(source);
		}
	}
}
