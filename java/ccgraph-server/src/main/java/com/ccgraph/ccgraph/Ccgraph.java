// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: ccgraph.proto

package com.ccgraph.ccgraph;

public final class Ccgraph {
  private Ccgraph() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_CCGraphRPC_StartParam_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_CCGraphRPC_StartParam_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_CCGraphRPC_CommandParam_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_CCGraphRPC_CommandParam_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_CCGraphRPC_CallParam_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_CCGraphRPC_CallParam_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_CCGraphRPC_RetRow_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_CCGraphRPC_RetRow_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_CCGraphRPC_Results_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_CCGraphRPC_Results_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\rccgraph.proto\022\nCCGraphRPC\"\014\n\nStartPara" +
      "m\"\037\n\014CommandParam\022\017\n\007command\030\001 \001(\014\"@\n\tCa" +
      "llParam\022\020\n\010txn_name\030\001 \001(\014\022\022\n\nparam_list\030" +
      "\002 \003(\014\022\r\n\005retry\030\003 \001(\010\"\031\n\006RetRow\022\017\n\007one_ro" +
      "w\030\001 \003(\014\"o\n\007Results\022\036\n\004code\030\001 \001(\0162\020.CCGra" +
      "phRPC.Code\022\020\n\010col_name\030\002 \003(\014\022!\n\005table\030\003 " +
      "\003(\0132\022.CCGraphRPC.RetRow\022\017\n\007measure\030\004 \003(\004" +
      "*6\n\004Code\022\007\n\003kOk\020\000\022\r\n\tkConflict\020\001\022\n\n\006kAbo" +
      "rt\020\002\022\n\n\006kFatal\020\0032\301\001\n\rCCGraphServer\022<\n\010Sh" +
      "utdown\022\026.CCGraphRPC.StartParam\032\026.CCGraph" +
      "RPC.StartParam\"\000\0226\n\006RunTxn\022\025.CCGraphRPC." +
      "CallParam\032\023.CCGraphRPC.Results\"\000\022:\n\007Comm" +
      "and\022\030.CCGraphRPC.CommandParam\032\023.CCGraphR" +
      "PC.Results\"\000B\027\n\023com.ccgraph.ccgraphP\001b\006p" +
      "roto3"
    };
    descriptor = com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        });
    internal_static_CCGraphRPC_StartParam_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_CCGraphRPC_StartParam_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_CCGraphRPC_StartParam_descriptor,
        new java.lang.String[] { });
    internal_static_CCGraphRPC_CommandParam_descriptor =
      getDescriptor().getMessageTypes().get(1);
    internal_static_CCGraphRPC_CommandParam_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_CCGraphRPC_CommandParam_descriptor,
        new java.lang.String[] { "Command", });
    internal_static_CCGraphRPC_CallParam_descriptor =
      getDescriptor().getMessageTypes().get(2);
    internal_static_CCGraphRPC_CallParam_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_CCGraphRPC_CallParam_descriptor,
        new java.lang.String[] { "TxnName", "ParamList", "Retry", });
    internal_static_CCGraphRPC_RetRow_descriptor =
      getDescriptor().getMessageTypes().get(3);
    internal_static_CCGraphRPC_RetRow_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_CCGraphRPC_RetRow_descriptor,
        new java.lang.String[] { "OneRow", });
    internal_static_CCGraphRPC_Results_descriptor =
      getDescriptor().getMessageTypes().get(4);
    internal_static_CCGraphRPC_Results_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_CCGraphRPC_Results_descriptor,
        new java.lang.String[] { "Code", "ColName", "Table", "Measure", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}
