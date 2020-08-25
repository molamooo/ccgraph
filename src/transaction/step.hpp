#pragma once

enum OperatorType {
  kNoOp = 0,

  kConst,

  kIntConst,
  // kIntRst, // 

  kNodeId, // provide label, id
  kGetNode, // give node label, id
  kGetAllNeighbor, // give edge label, id1, get all neighbour id
  kGetAllNeighborVarLen, // give node label, id
  kGetAllEdge, // give edge label, id1, get all edges
  kGetSingleEdge,
  kGetEdgeVia, // give edge label, id1, id2(s) or property(s)

  kCreateNode, 
  kUpdateNode,
  kDeleteNode,

  kCreateEdge,
  kUpdateEdge,
  kDeleteEdge,
  kUpsertEdge,

  kGroupBy,
  kSort,
  kFilter,
  kFilterSimple,
  kFilterByLabel,

  // kFilterNode, // child: node list, oprand. filter via
  // kFilterEdge, // child: node list, oprand. filter via

  kPlaceProp, // place a property on result col
  kPlacePropBack, // place a col back to property

  // kIntOp, //

  kAlgeo,

  kIf, // should also contain the end nodes of each branch. set the endif node's dependency.
  kEndIf, 

  kMerge, // merge results
};