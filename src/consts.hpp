#pragma once
enum CompareOp {
  kEq=0,
  kNe,
  kL,
  kLE,
  kG,
  kGe,
};

enum RetCode {
  kOk = 0,
  kAbort,
  kFatal,
};