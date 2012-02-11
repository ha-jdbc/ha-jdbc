package net.sf.hajdbc.codec;

import java.io.Serializable;
import java.sql.SQLException;

import net.sf.hajdbc.codec.base64.Base64CodecFactory;
import net.sf.hajdbc.codec.crypto.CipherCodecFactory;
import net.sf.hajdbc.codec.hex.HexCodecFactory;
import net.sf.hajdbc.codec.simple.SimpleCodecFactory;

/**
 * Codec factory that whose decoding behavior is determined by a prefix.
 * @author Paul Ferraro
 */
public class MultiplexingCodecFactory implements CodecFactory, Serializable {

	private static final long serialVersionUID = 4413927326976263687L;

	private static enum CodecFactoryEnum
	{
		HEX(new HexCodecFactory(), "H:"),
		BASE64(new Base64CodecFactory(), "B:"),
		CIPHER(new CipherCodecFactory(), "C:"),
		SIMPLE(new SimpleCodecFactory(), null),
		;
		private final CodecFactory factory;
		private final String prefix;
		
		private CodecFactoryEnum(CodecFactory factory, String prefix)
		{
			this.factory = factory;
			this.prefix = prefix;
		}

		String getPrefix()
		{
			return this.prefix;
		}
		
		CodecFactory getCodecFactory()
		{
			return this.factory;
		}
	}

	@Override
	public Codec createCodec(String clusterId) throws SQLException
	{
		return new MultiplexingCodec(clusterId);
	}

	private static class MultiplexingCodec implements Codec
	{
		private final String clusterId;
		
		MultiplexingCodec(String clusterId)
		{
			this.clusterId = clusterId;
		}
		
		@Override
		public String decode(String value) throws SQLException
		{
			CodecFactoryEnum factory = this.parseCodeFactory(value);
			String prefix = factory.getPrefix();
			String source = (prefix != null) ? value.substring(prefix.length()) : value;
			return factory.getCodecFactory().createCodec(this.clusterId).decode(source);
		}
		
		private CodecFactoryEnum parseCodeFactory(String value)
		{
			for (CodecFactoryEnum factory: CodecFactoryEnum.values())
			{
				String prefix = factory.getPrefix();
				if ((prefix != null) && value.startsWith(prefix))
				{
					return factory;
				}
			}
			return CodecFactoryEnum.SIMPLE;
		}

		@Override
		public String encode(String value) throws SQLException
		{
			throw new UnsupportedOperationException();
		}
	}
}
