package org.apache.spark.shuffle.ucx

class UcxStreamState(val callback: OperationCallback,
                     val request: UcxRequest,
                     var remaining: Int) {}

class UcxSliceState(val callback: OperationCallback,
                    val request: UcxRequest,
                    val mem: MemoryBlock,
                    var offset: Long,
                    var remaining: Int) {}
