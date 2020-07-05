#pragma once

enum OperatorType {
  kNoOp = 0,

  kIntConst,
  // kIntRst, // 

  kNodeId, // provide label, id
  kGetNode, // give node label, id
  kGetAllNeighbor, // give edge label, id1, get all neighbour id
  kGetAllEdge, // give edge label, id1, get all edges
  kGetEdgeVia, // give edge label, id1, id2(s) or property(s)

  kCreateNode,
  kUpdateNode,
  kDeleteNode,

  kCreateEdge,
  kUpdateEdge,
  kDeleteEdge,

  kFilterVia, // child: node list, oprand. filter via

  kIntOp, //

  kIf, // should also contain the end nodes of each branch. set the endif node's dependency.
  kEndIf, 

  kMerge, // merge results
};