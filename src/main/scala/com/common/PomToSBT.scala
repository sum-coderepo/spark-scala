package com.common
import scala.io.Source
import scala.xml._
object PomToSBT {
  def main (args : Array[String]) = {
    
    //build.sbt file
(XML.load("C:\\Program Files (x86)\\Jenkins\\workspace\\Spark_jenkins\\pom.xml") \\ "dependencies") \ "dependency" foreach ((dependency: Node) => {
val groupId = (dependency \ "groupId").text
val artifactId = (dependency \ "artifactId").text
val version = (dependency \ "version").text
val scope = (dependency \ "scope").text
val classifier = (dependency \ "classifier").text
val artifactValName: String = artifactId.replaceAll("[-\\.]", "_")

print("val %s = \"%s\" %% \"%s\" %% \"%s\"".format(artifactValName, groupId, artifactId, version))
scope match {
  case "" => print("\n")
  case _ => print(" %% \"%s\"\n".format(scope))
}
None
});
  }
}