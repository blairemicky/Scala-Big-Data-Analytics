import org.apache.log4j.{Logger, Level}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import org.apache.hadoop.conf.Configuration
import org.bson.BSONObject

import com.mongodb.{
    MongoClient,
    MongoException,
    WriteConcern,
    DB,
    DBCollection,
    BasicDBObject,
    BasicDBList,
    DBObject,
    DBCursor
}

import com.mongodb.hadoop.{
    MongoInputFormat,
    MongoOutputFormat,
    BSONFileInputFormat,
    BSONFileOutputFormat
}

import com.mongodb.hadoop.io.MongoUpdateWritable


object MongoSpark {
    def main(args: Array[String]) {
        /* Uncomment to turn off Spark logs */
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val conf = new SparkConf()
          .setMaster("local[*]")
          .setAppName("MongoSpark")
          .set("spark.driver.memory", "2g")
          .set("spark.executor.memory", "4g")

        val sc = new SparkContext(conf)

        val article_input_conf = new Configuration()
        article_input_conf.set("mongo.input.uri", "mongodb://localhost:27017/dblp.article")//name changed

        val inproceedings_input_conf = new Configuration()
        inproceedings_input_conf.set("mongo.input.uri", "mongodb://localhost:27017/dblp.inproceedings")//name changed

        val article = sc.newAPIHadoopRDD(
            article_input_conf,         // config
            classOf[MongoInputFormat],  // input format
            classOf[Object],            // key type
            classOf[BSONObject]         // val type
        )

        val inproceedings = sc.newAPIHadoopRDD(
            inproceedings_input_conf,
            classOf[MongoInputFormat],
            classOf[Object],
            classOf[BSONObject]
        )

        // question numbers correspond to those in HW1
        q2(inproceedings)
        q3b(inproceedings)
        q3d(article, inproceedings)
        q4b(article, inproceedings)
    }

    /* Q2.
     * Add a column "Area" in the Inproceedings table.
     * Then, populate the column "Area" with the values from the above table if
     * there is a match, otherwise set it to "UNKNOWN" */
    def q2(inproceedings: RDD[(Object,BSONObject)]) {

        val output_conf = new Configuration()
        output_conf.set("mongo.output.uri", "mongodb://localhost:27017/dblp.inproceedings")

        val database = List("SIGMOD Conference","VLDB","ICDE","PODS")
        val theory = List("STOC","FOCS","SODA","ICALP")
        val systems = List("SIGCOMM","ISCA","HPCA","PLDI")
        val ml_ai = List("ICML","NIPS","AAAI","IJCAI")
        val up_database = inproceedings.filter(va => database.contains(va._2.get("booktitle")))
          .mapValues(value => new MongoUpdateWritable(
              new BasicDBObject("_id",value.get("_id")),
              new BasicDBObject("$set", new BasicDBObject("Area","Database")),
              false, false, false))
        val up_theory = inproceedings.filter(va => theory.contains(va._2.get("booktitle")))
          .mapValues(value => new MongoUpdateWritable(
              new BasicDBObject("_id",value.get("_id")),
              new BasicDBObject("$set", new BasicDBObject("Area","Theory")),
              false, false, false))

        val up_system = inproceedings.filter(va => systems.contains(va._2.get("booktitle")))
          .mapValues(value => new MongoUpdateWritable(
              new BasicDBObject("_id",value.get("_id")),
              new BasicDBObject("$set", new BasicDBObject("Area","Systems")),
              false, false, false))

        val up_ml = inproceedings.filter(va => ml_ai.contains(va._2.get("booktitle")))
          .mapValues(value => new MongoUpdateWritable(
              new BasicDBObject("_id",value.get("_id")),
              new BasicDBObject("$set", new BasicDBObject("Area","ML_AI")),
              false, false, false))

           val known = database:::theory:::systems:::ml_ai
            val up_un = inproceedings.filter(va => !known.contains(va._2.get("booktitle")))
          .mapValues(value => new MongoUpdateWritable(
              new BasicDBObject("_id",value.get("_id")),
              new BasicDBObject("$set", new BasicDBObject("Area","UNKNOWN")),
              false, false, false))
            up_database.union(up_ml).union(up_system).union(up_theory).union(up_un)
            .saveAsNewAPIHadoopFile("file:///this-is-completely-unused",
            classOf[Object], classOf[MongoUpdateWritable],
            classOf[MongoOutputFormat[Object, MongoUpdateWritable]],
            output_conf)

    }



    /* Q3b.
     * Find the TOP­20 authors who published the most number of papers in
     * "Database" (author published in multiple areas will be counted in all those
     * areas). */
  /*
("Divesh Srivastava",137)
("Surajit Chaudhuri",118)
("Jiawei Han 0001",109)
("Jeffrey F. Naughton",107)
("Philip S. Yu",106)
("Hector Garcia-Molina",103)
("H. V. Jagadish",102)
("Raghu Ramakrishnan",98)
("Beng Chin Ooi",90)
("Michael Stonebraker",89)
("Kian-Lee Tan",87)
("Rakesh Agrawal",81)
("Michael J. Carey",81)
("Nick Koudas",80)
("David J. DeWitt",79)
("Michael J. Franklin",76)
("Christos Faloutsos",75)
("Jeffrey Xu Yu",72)
("Dan Suciu",72)
("Gerhard Weikum",72)
   */
    def q3b(inproceedings: RDD[(Object,BSONObject)]) {
      val author_inp = inproceedings.filter(rec => rec._2.get("Area") == "Database")
      val outtable = author_inp
        .map{case(id,l) => l.get("author").toString.stripPrefix("[").stripSuffix("]").trim()}
        .flatMap(x => x.split(" , "))
        .map(x => (x,1)).reduceByKey(_+_).sortBy(_._2,ascending = false)
      outtable.take(20).foreach(println)

    }



  /* Q3d.
     * Find the number of authors who wrote more journal papers than conference
     * papers (irrespective of research areas). */

  // 801767

  def q3d(article: RDD[(Object,BSONObject)], inproceedings: RDD[(Object,BSONObject)]) {
    val author_inp = inproceedings
      .map{case(id,l) => l.get("author").toString.stripPrefix("[").stripSuffix("]").trim()}
      .flatMap(x => x.split(" , ")).map{case(x) => (x,1)}.reduceByKey(_+_)

    val author_art = article
      .map{case(id,l) => l.get("author").toString.stripPrefix("[").stripSuffix("]").trim()}
      .flatMap(x => x.split(" , ")).map{case(x) => (x,1)}.reduceByKey(_+_)
    val num = author_art.leftOuterJoin(author_inp)
      .map{case (author,(jour,conf)) => (author,jour,conf)}
      .map{case (author,jour,conf) => if(conf.isEmpty) (author,jour,Some(0)) else (author,jour,conf)}
      .map{case (author,jour,Some(conf)) => (author,jour,conf)}
      .filter{case (author,jour,conf) => jour> conf}.count()

    println(num)

  }

    /* Q4b.
     * Plot a barchart showing how the average number of collaborators varied in
     * these decades for conference papers in each of the four areas in Q3.
     * Again, the decades will be 1950-1959, 1960-1969, ...., 2000-2009, 2010-2015.
     * But for every decade, there will be four bars one for each area (do not
     * consider UNKNOWN), the height of the bars will denote the average number of
     * collaborators. */
   /*
  (1960,Theory,2.1)
  (1960,ML_AI,2.3246753)
  (1970,Theory,4.9471545)
  (1970,Database,5.8032036)
  (1970,Systems,4.53937)
  (1970,ML_AI,4.0141754)
  (1980,Systems,7.069128)
  (1980,Database,8.5821085)
  (1980,ML_AI,5.5522447)
  (1980,Theory,9.682791)
  (1990,Systems,14.996128)
  (1990,Theory,15.775213)
  (1990,Database,14.389069)
  (1990,ML_AI,11.18111)
  (2000,Database,29.769766)
  (2000,ML_AI,25.906555)
  (2000,Systems,30.58957)
  (2000,Theory,25.541553)
  (2010,Theory,33.035835)
  (2010,Systems,53.357082)
  (2010,ML_AI,47.39261)
  (2010,Database,54.695335)
  */
    def q4b(article: RDD[(Object,BSONObject)], inproceedings: RDD[(Object,BSONObject)]) {

        val author_inp = inproceedings
          .map{case(id, l) => (id, l.get("author").toString.stripPrefix("[").stripSuffix("]").trim())}
          .flatMapValues(authors => authors.split(" , "))
        val author_art = article
          .map{case(id, l) => (id, l.get("author").toString.stripPrefix("[").stripSuffix("]").trim())}
          .flatMapValues(authors => authors.split(" , "))
        val all_authors = author_art.union(author_inp).filter( x => x._2!="")

        val art = article.filter(x => x._2.get("year")!= "null")
          .map{ case(id, l) => (id, l.get("year").toString.toInt)}
          .map{ case(key, year) => (key, Math.floor(year/10)*10)}
          .map{ case(key, decade) => (key, decade.toInt)}
        val inp = inproceedings.filter(x => x._2.get("year")!= "null")
          .map{ case(id, content) => (id, content.get("year").toString.toInt)}
          .map{ case(key, year) => (key, Math.floor(year/10)*10)}
          .map{ case(key, decade) => (key, decade.toInt)}
        val art_u_inp= art.union(inp)

        val tab0 = inproceedings.filter(x => x._2.get("Area")!="UNKNOWN")
       .map{ case(id, l) => (l.get("Area"), l.get("year").toString.toInt,
         l.get("author").toString.stripPrefix("[").stripSuffix("]").trim())}
       .filter{ case (area,year,author) => year > 1950}
       .map{ case(area, year, authors) => ((area, (Math.floor(year/10)*10).toInt), authors)}
       .flatMapValues( authors => authors.split(" , ")).distinct
       .filter{case((area, decade), author) => author!=""}
       .map{case((area, decade), author) => ((author, decade),area)}

        // join the all_authors table with all_authors and author1 is unequal to author2.
        val tab1 = all_authors.join(all_authors)
          .filter{case (id,(au1,au2)) => au1 != au2}.join(art_u_inp)
          .map{case(id,((au1,au2),decade)) => (au1, au2, decade)}.distinct
          .map{case(au1, au2, decade) => ((au1, decade),1)}
          .reduceByKey(_ + _)

       val output = tab1.join(tab0).map{
         case ((author,decade),(n,area)) => ((decade,area),n)}
         .combineByKey((va) => (va, 1), (a: (Int,Int), va) => (a._1 + va, a._2 + 1),
         (a: (Int,Int), b: (Int,Int)) => (a._1 + b._1, a._2 + b._2))
         .map{case (label,va) => (label, va._1/va._2.toFloat)}
         .map{case ((a,b),c) => (a,b.toString,c)}
         .sortBy(_._1)

         output.collect().foreach(println)



    }
}