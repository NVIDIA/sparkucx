package org.apache.spark.shuffle.ucx

class UcxStreamState(val callback: OperationCallback,
                     val request: UcxRequest,
                     var remaining: Int) {}
