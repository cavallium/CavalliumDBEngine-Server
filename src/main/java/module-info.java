module dbengine.server {
	requires it.unimi.dsi.fastutil;
	requires org.apache.logging.log4j;
	requires reactor.netty.incubator.quic;
	requires reactor.netty.core;
	requires dbengine;
	requires java.logging;
	requires reactor.core;
	requires org.reactivestreams;
	requires micrometer.core;
	requires io.netty5.buffer;
	requires org.jetbrains.annotations;
	requires io.netty.incubator.codec.classes.quic;
	requires io.netty.handler;

}