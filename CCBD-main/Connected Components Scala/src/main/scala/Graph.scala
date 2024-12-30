import org.apache.spark._
import org.apache.spark.rdd._

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.math.min
@SerialVersionUID(123L)
case class Object2001(node_x:Long, group_x:Long, group_y:Long) extends Serializable{}
   object Graph {
   def main ( args: Array[String] ) {
      val conf = new SparkConf().setAppName("hprakash1_Graph")
      val sc = new SparkContext(conf)

      // A graph is a dataset of vertices, where each vertex is a triple
      //   (group,id,adj) where id is the vertex id, group is the group id
      //   (initially equal to id), and adj is the list of outgoing neighbors
      var graph: RDD[ ( Long, Long, List[Long] ) ]
         = sc.textFile(args(0)).map( line => { val l_s = line.split(",")
                              (l_s(0).toLong,l_s(0).toLong,l_s.drop(1).toList.map(_.toLong)) })     // read the graph from the file args(0)
      var make_graph = graph.map(connected_graph => (connected_graph._1,connected_graph))
      //make_graph.foreach(println)
         
      for ( i <- 1 to 5 ) {
         // For each vertex (group,id,adj) generate the candidate (id,group)
         //    and for each x in adj generate the candidate (x,group).
         // Then for each vertex, its new group number is the minimum candidate
         //val groups: RDD[ ( Long, Long ) ]
         graph = graph.flatMap(map => map match{ case (k, l, kl) => (k, l) :: kl.map(m => (m,l))})
            .reduceByKey((x, y) => (if (x >= y) y else x))
            .join(make_graph).map(grph => (grph._2._2._2, grph._2._1, grph._2._2._3))
      }

         // reconstruct the graph using the new group numbers
         //graph = groups.

      // print the group sizes
      val groups = graph.map(connected_graph => (connected_graph._2, 1))
      .reduceByKey((p, q) => (p + q))
      .sortBy(_._1)
      .collect()
      .foreach(println)
      sc.stop()
  }
}
