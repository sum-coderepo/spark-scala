package com.dedup

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

private[dedup] object SerializableHelpers {
  def getKeysFromRow(row: GenericRowWithSchema, primaryKeys: Seq[String]): Seq[String] =
    primaryKeys.map(fieldName => row.getAs[String](fieldName))
}