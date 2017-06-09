package com.config
import com.typesafe.config.{ConfigObject, ConfigValue, ConfigFactory, Config}  
import scala.collection.JavaConverters._  
import java.net.URI  
import java.util.Map.Entry

case class Settings(config: Config) {  
  lazy val decoders : Map[String, URI] = {
    val list : Iterable[ConfigObject] = config.getObjectList("decoders").asScala
    (for {
      item : ConfigObject <- list
      entry : Entry[String, ConfigValue] <- item.entrySet().asScala
      key = entry.getKey
      uri = new URI(entry.getValue.unwrapped().toString)
    } yield (key, uri)).toMap
  }
}