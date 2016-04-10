package org.apache.flink.runtime.io.network.netty;

import io.netty.channel.*;
import io.netty.util.concurrent.EventExecutorGroup;

import java.net.SocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by winston on 10/04/2016.
 */
public class MagicPipeline implements ChannelPipeline {

	private final PartitionRequestClientHandler handler;

	public MagicPipeline(PartitionRequestClientHandler handler) {
		this.handler = handler;
	}

	@Override
	public ChannelPipeline addFirst(String name, ChannelHandler handler) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelPipeline addFirst(EventExecutorGroup group, String name, ChannelHandler handler) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelPipeline addLast(String name, ChannelHandler handler) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelPipeline addLast(EventExecutorGroup group, String name, ChannelHandler handler) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelPipeline addBefore(String baseName, String name, ChannelHandler handler) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelPipeline addBefore(EventExecutorGroup group, String baseName, String name, ChannelHandler handler) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelPipeline addAfter(String baseName, String name, ChannelHandler handler) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelPipeline addAfter(EventExecutorGroup group, String baseName, String name, ChannelHandler handler) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelPipeline addFirst(ChannelHandler... handlers) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelPipeline addFirst(EventExecutorGroup group, ChannelHandler... handlers) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelPipeline addLast(ChannelHandler... handlers) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelPipeline addLast(EventExecutorGroup group, ChannelHandler... handlers) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelPipeline remove(ChannelHandler handler) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelHandler remove(String name) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T extends ChannelHandler> T remove(Class<T> handlerType) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelHandler removeFirst() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelHandler removeLast() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelPipeline replace(ChannelHandler oldHandler, String newName, ChannelHandler newHandler) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelHandler replace(String oldName, String newName, ChannelHandler newHandler) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T extends ChannelHandler> T replace(Class<T> oldHandlerType, String newName, ChannelHandler newHandler) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelHandler first() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelHandlerContext firstContext() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelHandler last() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelHandlerContext lastContext() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelHandler get(String name) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T extends ChannelHandler> T get(Class<T> handlerType) {
		if (handlerType.equals(PartitionRequestClientHandler.class)) {
			return (T) handler;
		}
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelHandlerContext context(ChannelHandler handler) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelHandlerContext context(String name) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelHandlerContext context(Class<? extends ChannelHandler> handlerType) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Channel channel() {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<String> names() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Map<String, ChannelHandler> toMap() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelPipeline fireChannelRegistered() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelPipeline fireChannelUnregistered() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelPipeline fireChannelActive() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelPipeline fireChannelInactive() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelPipeline fireExceptionCaught(Throwable cause) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelPipeline fireUserEventTriggered(Object event) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelPipeline fireChannelRead(Object msg) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelPipeline fireChannelReadComplete() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelPipeline fireChannelWritabilityChanged() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture bind(SocketAddress localAddress) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture connect(SocketAddress remoteAddress) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture disconnect() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture close() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture deregister() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture disconnect(ChannelPromise promise) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture close(ChannelPromise promise) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture deregister(ChannelPromise promise) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelPipeline read() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture write(Object msg) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture write(Object msg, ChannelPromise promise) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelPipeline flush() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture writeAndFlush(Object msg) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<Map.Entry<String, ChannelHandler>> iterator() {
		throw new UnsupportedOperationException();
	}

}
