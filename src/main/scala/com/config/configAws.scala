package com.config

import com.amazonaws.encryptionsdk.AwsCrypto
import com.amazonaws.encryptionsdk.kms.KmsMasterKeyProvider
import com.amazonaws.encryptionsdk.CryptoResult
import com.amazonaws.encryptionsdk.kms.KmsMasterKey

import scala.collection.immutable.Map
import java.util.Collections
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.Row
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

class Connection(name:String, connType:String, properties:Map[String,String]) extends Serializable {
  val Name = name
  val Type = connType
  var Properties = properties
  
  def toJson() : String ={
    val json = ("Name" -> Name) ~ ("Type" -> Type) ~ ("Properties" -> Properties)
    compact(render(json))
  }
  
  def withPropertyToEncryt(keyArn:String, propKey:String, propValue:String) : Connection = {
    val encryptedKey = s"$propKey.encrypted"
    val encryptedValue = encryptText(keyArn, propValue)
    Properties = Properties.+(encryptedKey -> encryptedValue)
    Properties = Properties.+(s"$propKey.encrypted.keyARN" -> keyArn)
    this
  }
  
  private def encryptText(keyArn:String, clearText : String) = {
    // Instantiate the SDK
    val crypto = new AwsCrypto()

    // Set up the KmsMasterKeyProvider backed by the default credentials
    val prov = new KmsMasterKeyProvider(keyArn)
    
    val context = Collections.singletonMap("connection-name", Name)
    crypto.encryptString(prov, clearText, context).getResult()
  }
  
  private def decryptCipherText(keyArn:String, cipherText : String) = {
    // Instantiate the SDK
    val crypto = new AwsCrypto()

    // Set up the KmsMasterKeyProvider backed by the default credentials
    val prov = new KmsMasterKeyProvider(keyArn)

    val decryptResult = crypto.decryptString(prov, cipherText)  
  
    // We need to check the encryption context (and ideally key) to ensure that
    // this was the ciphertext we expected
    if (!decryptResult.getMasterKeyIds().get(0).equals(keyArn)) {
      throw new IllegalStateException("Wrong key id!")
    }
  
    // The SDK may add information to the encryption context, so we check to ensure
    // that all of our values are present
    val context = Map("connection-name" -> Name)
    val contextEntrySetSize = context.keySet.size
    val contextEntrySetIter = context.keysIterator
    var index = 0
    while(index < contextEntrySetSize) {
      //val nextMap = contextEntrySetIter.next()
      val nextKey = contextEntrySetIter.next
      val nextValue = context.get(nextKey).getOrElse("")
      if (!nextValue.equals(decryptResult.getEncryptionContext().get(nextKey))) {
        throw new IllegalStateException("Wrong Encryption Context!")
      }
      index = index + 1
    }  
    //return decrypted result
    decryptResult.getResult()
  }
  
  private def getPropertyValue(propName:String) : String = {
    var propValue : String = ""
    if(Properties.contains(s"$propName.encrypted")) {
      if(!Properties.contains(s"$propName.encrypted.keyARN")) {
        //encrypted value with no ARN
        throw new Exception(s"Can Not Find KeyARN for Encrypted Property $propName")
      }
      val keyArn = Properties.get(s"$propName.encrypted.keyARN").getOrElse("")
      val cipherText = Properties.get(s"$propName.encrypted").getOrElse("")
      propValue = decryptCipherText(keyArn, cipherText)
    } else {
      propValue = Properties.get(propName).getOrElse("")
    }
    propValue
  }
  
  private def createJdbcUrl() = {
    val driver = getPropertyValue("driver")
    val hostname = getPropertyValue("hostname")
    val port = getPropertyValue("port")
    val username = getPropertyValue("username")
    val database = getPropertyValue("database")
    val password = getPropertyValue("password")
    s"jdbc:$driver://$hostname:$port/$database?user=$username&password=$password"  
  }  

  def withReader(reader:DataFrameReader) : DataFrameReader = {
    var mutableReader = reader
  
    if(Type == "jdbc") {
      val jdbcUrl = createJdbcUrl()
      mutableReader = reader.format("jdbc").option("url",jdbcUrl)
    } else if(Type == "file") {
      val path = Properties.get("path").getOrElse("")
      mutableReader = reader.option("path",path)
    }
    mutableReader
  }  
  
  def withWriter(writer:DataFrameWriter[Row]) : DataFrameWriter[Row] = {
    var mutableWriter = writer
    
    if(Type == "jdbc") {
      val jdbcUrl = createJdbcUrl()
      mutableWriter = writer.format("jdbc").option("url",jdbcUrl)
    } else if(Type == "file") {
      val path = Properties.get("path").getOrElse("")
      mutableWriter = writer.option("path",path)
    }
    mutableWriter
  }
}
package com.databricks.solutions.etl.config

class JobConfig(name:String, connections:Array[Connection] = Seq().toArray) {
  val Name = name
  val Connections = connections
  
  def toJson() : String = {
    val connectionsJson = new StringBuilder
    val dQuote = "\""
    
    connectionsJson++= "{"+dQuote+"Name"+dQuote+":"+dQuote+Name+dQuote
    
    if(Connections.size > 0){
      connectionsJson++= ","+dQuote+"Connections"+dQuote+":["
      for(conn <- Connections) {
        if(conn != Connections(0)){
          connectionsJson++= "," + conn.toJson    
        } else {
          connectionsJson++= conn.toJson
        }
      }
      connectionsJson++= "]"
    }
    connectionsJson++= "}"
    connectionsJson.mkString
  }
  
  def getConnection(name:String) = {
    Connections.find(_.Name == (name))
  }
  
  def withConnection(connection : Connection) : JobConfig = {
    val newConnections = this.Connections :+ connection
    return new JobConfig(this.Name, newConnections)
  }
  
  override def toString() : String = {
    this.toJson()
  }
}