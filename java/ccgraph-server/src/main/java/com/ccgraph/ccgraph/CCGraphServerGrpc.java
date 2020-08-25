package com.ccgraph.ccgraph;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 * <pre>
 * message TxnIDParam {
 *   uint64 txnid = 1;
 *   bool reuse = 2;
 * }
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.31.1)",
    comments = "Source: ccgraph.proto")
public final class CCGraphServerGrpc {

  private CCGraphServerGrpc() {}

  public static final String SERVICE_NAME = "CCGraphRPC.CCGraphServer";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.ccgraph.ccgraph.StartParam,
      com.ccgraph.ccgraph.StartParam> getShutdownMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Shutdown",
      requestType = com.ccgraph.ccgraph.StartParam.class,
      responseType = com.ccgraph.ccgraph.StartParam.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.ccgraph.ccgraph.StartParam,
      com.ccgraph.ccgraph.StartParam> getShutdownMethod() {
    io.grpc.MethodDescriptor<com.ccgraph.ccgraph.StartParam, com.ccgraph.ccgraph.StartParam> getShutdownMethod;
    if ((getShutdownMethod = CCGraphServerGrpc.getShutdownMethod) == null) {
      synchronized (CCGraphServerGrpc.class) {
        if ((getShutdownMethod = CCGraphServerGrpc.getShutdownMethod) == null) {
          CCGraphServerGrpc.getShutdownMethod = getShutdownMethod =
              io.grpc.MethodDescriptor.<com.ccgraph.ccgraph.StartParam, com.ccgraph.ccgraph.StartParam>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Shutdown"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ccgraph.ccgraph.StartParam.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ccgraph.ccgraph.StartParam.getDefaultInstance()))
              .setSchemaDescriptor(new CCGraphServerMethodDescriptorSupplier("Shutdown"))
              .build();
        }
      }
    }
    return getShutdownMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.ccgraph.ccgraph.CallParam,
      com.ccgraph.ccgraph.Results> getRunTxnMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RunTxn",
      requestType = com.ccgraph.ccgraph.CallParam.class,
      responseType = com.ccgraph.ccgraph.Results.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.ccgraph.ccgraph.CallParam,
      com.ccgraph.ccgraph.Results> getRunTxnMethod() {
    io.grpc.MethodDescriptor<com.ccgraph.ccgraph.CallParam, com.ccgraph.ccgraph.Results> getRunTxnMethod;
    if ((getRunTxnMethod = CCGraphServerGrpc.getRunTxnMethod) == null) {
      synchronized (CCGraphServerGrpc.class) {
        if ((getRunTxnMethod = CCGraphServerGrpc.getRunTxnMethod) == null) {
          CCGraphServerGrpc.getRunTxnMethod = getRunTxnMethod =
              io.grpc.MethodDescriptor.<com.ccgraph.ccgraph.CallParam, com.ccgraph.ccgraph.Results>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RunTxn"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ccgraph.ccgraph.CallParam.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ccgraph.ccgraph.Results.getDefaultInstance()))
              .setSchemaDescriptor(new CCGraphServerMethodDescriptorSupplier("RunTxn"))
              .build();
        }
      }
    }
    return getRunTxnMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.ccgraph.ccgraph.CommandParam,
      com.ccgraph.ccgraph.Results> getCommandMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Command",
      requestType = com.ccgraph.ccgraph.CommandParam.class,
      responseType = com.ccgraph.ccgraph.Results.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.ccgraph.ccgraph.CommandParam,
      com.ccgraph.ccgraph.Results> getCommandMethod() {
    io.grpc.MethodDescriptor<com.ccgraph.ccgraph.CommandParam, com.ccgraph.ccgraph.Results> getCommandMethod;
    if ((getCommandMethod = CCGraphServerGrpc.getCommandMethod) == null) {
      synchronized (CCGraphServerGrpc.class) {
        if ((getCommandMethod = CCGraphServerGrpc.getCommandMethod) == null) {
          CCGraphServerGrpc.getCommandMethod = getCommandMethod =
              io.grpc.MethodDescriptor.<com.ccgraph.ccgraph.CommandParam, com.ccgraph.ccgraph.Results>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Command"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ccgraph.ccgraph.CommandParam.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.ccgraph.ccgraph.Results.getDefaultInstance()))
              .setSchemaDescriptor(new CCGraphServerMethodDescriptorSupplier("Command"))
              .build();
        }
      }
    }
    return getCommandMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static CCGraphServerStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<CCGraphServerStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<CCGraphServerStub>() {
        @java.lang.Override
        public CCGraphServerStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new CCGraphServerStub(channel, callOptions);
        }
      };
    return CCGraphServerStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static CCGraphServerBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<CCGraphServerBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<CCGraphServerBlockingStub>() {
        @java.lang.Override
        public CCGraphServerBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new CCGraphServerBlockingStub(channel, callOptions);
        }
      };
    return CCGraphServerBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static CCGraphServerFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<CCGraphServerFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<CCGraphServerFutureStub>() {
        @java.lang.Override
        public CCGraphServerFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new CCGraphServerFutureStub(channel, callOptions);
        }
      };
    return CCGraphServerFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * message TxnIDParam {
   *   uint64 txnid = 1;
   *   bool reuse = 2;
   * }
   * </pre>
   */
  public static abstract class CCGraphServerImplBase implements io.grpc.BindableService {

    /**
     */
    public void shutdown(com.ccgraph.ccgraph.StartParam request,
        io.grpc.stub.StreamObserver<com.ccgraph.ccgraph.StartParam> responseObserver) {
      asyncUnimplementedUnaryCall(getShutdownMethod(), responseObserver);
    }

    /**
     */
    public void runTxn(com.ccgraph.ccgraph.CallParam request,
        io.grpc.stub.StreamObserver<com.ccgraph.ccgraph.Results> responseObserver) {
      asyncUnimplementedUnaryCall(getRunTxnMethod(), responseObserver);
    }

    /**
     */
    public void command(com.ccgraph.ccgraph.CommandParam request,
        io.grpc.stub.StreamObserver<com.ccgraph.ccgraph.Results> responseObserver) {
      asyncUnimplementedUnaryCall(getCommandMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getShutdownMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.ccgraph.ccgraph.StartParam,
                com.ccgraph.ccgraph.StartParam>(
                  this, METHODID_SHUTDOWN)))
          .addMethod(
            getRunTxnMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.ccgraph.ccgraph.CallParam,
                com.ccgraph.ccgraph.Results>(
                  this, METHODID_RUN_TXN)))
          .addMethod(
            getCommandMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.ccgraph.ccgraph.CommandParam,
                com.ccgraph.ccgraph.Results>(
                  this, METHODID_COMMAND)))
          .build();
    }
  }

  /**
   * <pre>
   * message TxnIDParam {
   *   uint64 txnid = 1;
   *   bool reuse = 2;
   * }
   * </pre>
   */
  public static final class CCGraphServerStub extends io.grpc.stub.AbstractAsyncStub<CCGraphServerStub> {
    private CCGraphServerStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CCGraphServerStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new CCGraphServerStub(channel, callOptions);
    }

    /**
     */
    public void shutdown(com.ccgraph.ccgraph.StartParam request,
        io.grpc.stub.StreamObserver<com.ccgraph.ccgraph.StartParam> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getShutdownMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void runTxn(com.ccgraph.ccgraph.CallParam request,
        io.grpc.stub.StreamObserver<com.ccgraph.ccgraph.Results> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getRunTxnMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void command(com.ccgraph.ccgraph.CommandParam request,
        io.grpc.stub.StreamObserver<com.ccgraph.ccgraph.Results> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCommandMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * message TxnIDParam {
   *   uint64 txnid = 1;
   *   bool reuse = 2;
   * }
   * </pre>
   */
  public static final class CCGraphServerBlockingStub extends io.grpc.stub.AbstractBlockingStub<CCGraphServerBlockingStub> {
    private CCGraphServerBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CCGraphServerBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new CCGraphServerBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.ccgraph.ccgraph.StartParam shutdown(com.ccgraph.ccgraph.StartParam request) {
      return blockingUnaryCall(
          getChannel(), getShutdownMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.ccgraph.ccgraph.Results runTxn(com.ccgraph.ccgraph.CallParam request) {
      return blockingUnaryCall(
          getChannel(), getRunTxnMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.ccgraph.ccgraph.Results command(com.ccgraph.ccgraph.CommandParam request) {
      return blockingUnaryCall(
          getChannel(), getCommandMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * message TxnIDParam {
   *   uint64 txnid = 1;
   *   bool reuse = 2;
   * }
   * </pre>
   */
  public static final class CCGraphServerFutureStub extends io.grpc.stub.AbstractFutureStub<CCGraphServerFutureStub> {
    private CCGraphServerFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CCGraphServerFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new CCGraphServerFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.ccgraph.ccgraph.StartParam> shutdown(
        com.ccgraph.ccgraph.StartParam request) {
      return futureUnaryCall(
          getChannel().newCall(getShutdownMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.ccgraph.ccgraph.Results> runTxn(
        com.ccgraph.ccgraph.CallParam request) {
      return futureUnaryCall(
          getChannel().newCall(getRunTxnMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.ccgraph.ccgraph.Results> command(
        com.ccgraph.ccgraph.CommandParam request) {
      return futureUnaryCall(
          getChannel().newCall(getCommandMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_SHUTDOWN = 0;
  private static final int METHODID_RUN_TXN = 1;
  private static final int METHODID_COMMAND = 2;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final CCGraphServerImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(CCGraphServerImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SHUTDOWN:
          serviceImpl.shutdown((com.ccgraph.ccgraph.StartParam) request,
              (io.grpc.stub.StreamObserver<com.ccgraph.ccgraph.StartParam>) responseObserver);
          break;
        case METHODID_RUN_TXN:
          serviceImpl.runTxn((com.ccgraph.ccgraph.CallParam) request,
              (io.grpc.stub.StreamObserver<com.ccgraph.ccgraph.Results>) responseObserver);
          break;
        case METHODID_COMMAND:
          serviceImpl.command((com.ccgraph.ccgraph.CommandParam) request,
              (io.grpc.stub.StreamObserver<com.ccgraph.ccgraph.Results>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class CCGraphServerBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    CCGraphServerBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.ccgraph.ccgraph.Ccgraph.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("CCGraphServer");
    }
  }

  private static final class CCGraphServerFileDescriptorSupplier
      extends CCGraphServerBaseDescriptorSupplier {
    CCGraphServerFileDescriptorSupplier() {}
  }

  private static final class CCGraphServerMethodDescriptorSupplier
      extends CCGraphServerBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    CCGraphServerMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (CCGraphServerGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new CCGraphServerFileDescriptorSupplier())
              .addMethod(getShutdownMethod())
              .addMethod(getRunTxnMethod())
              .addMethod(getCommandMethod())
              .build();
        }
      }
    }
    return result;
  }
}
