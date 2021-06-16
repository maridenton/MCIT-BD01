package Assign1

import java.sql.{Connection, DriverManager, ResultSet}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class Main extends App {

  val lPath: String = "/home/marinda/Downloads/gtfs_stm"
  val fsPath: String = "/user/fall2019/marinda/project4"
  val dbName: String = "fall2019_marinda"

  // ==== Prepare File system
  val conf = new Configuration()
  conf.addResource(new Path("/home/marinda/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
  conf.addResource(new Path("/home/marinda/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))
  val fs: FileSystem = FileSystem.get(conf)

  setupFileSystem(fsPath)

  // ==== Prepare DB staging area

  val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)

  val connection: Connection = DriverManager.getConnection("jdbc:hive2://172.16.129.58:10000/;user=marinda;password=marinda")
  val stmt = connection.createStatement()

  setupStagingArea(dbName)
  populateStagingArea(lPath, fsPath)
  enrichTrip(dbName)

  stmt.close()
  connection.close()
  println("Done")

  // =================================================================================================================
  def setupFileSystem(fPath: String) = {

    println("Setting up HDFS")

    createFolder(new Path(fPath + "/trips"))
    createFolder(new Path(fPath + "/calendar_dates"))
    createFolder(new Path(fPath + "/frequencies"))
  }
  // =================================================================================================================
  def createFolder(fPath: Path) = {
    if (fs.isDirectory(fPath)) {
      println(s"Deleting folder $fPath")
      fs.delete(fPath, true)
    }
    fs.mkdirs(fPath)
  }
  // =================================================================================================================
  def setupStagingArea(dbName: String) = {
    println("Setting up Staging area")
    stmt.execute("SET hive.exec.dynamic.partition.mode=nonstrict")
    stmt.execute("CREATE DATABASE IF NOT EXISTS " + dbName)
    stmt.execute("USE " + dbName)

    stmt.execute("CREATE EXTERNAL TABLE IF NOT EXISTS " + dbName + ".ext_trips ( " +
      " route_id SMALLINT," +
      " service_id STRING, " +
      " trip_id STRING," +
      " trip_headsign STRING," +
      " direction_id TINYINT," +
      " shape_id INT," +
      " wheelchair_accessible TINYINT," +
      " note_fr STRING," +
      " note_en STRING)" +
      " ROW FORMAT DELIMITED " +
      " FIELDS TERMINATED BY ',' " +
      " STORED AS TEXTFILE " +
      " LOCATION '/user/fall2019/marinda/project4/trips' " +
      " TBLPROPERTIES ('skip.header.line.count' = '1', 'serialization.null.format' = '')")

    stmt.execute("CREATE EXTERNAL TABLE IF NOT EXISTS " + dbName + ".ext_calendar_dates ( " +
      " service_id STRING, " +
      " date STRING, " +
      " exception_type TINYINT) " +
      " ROW FORMAT DELIMITED " +
      " FIELDS TERMINATED BY ',' " +
      " STORED AS TEXTFILE " +
      " LOCATION '/user/fall2019/marinda/project4/calendar_dates' " +
      " TBLPROPERTIES ('skip.header.line.count' = '1', 'serialization.null.format' = '')")

    stmt.execute("CREATE EXTERNAL TABLE IF NOT EXISTS " + dbName + ".ext_frequencies ( " +
      " trip_id STRING," +
      " start_time STRING, " +
      " end_time STRING, " +
      " headway_secs SMALLINT) " +
      " ROW FORMAT DELIMITED " +
      " FIELDS TERMINATED BY ',' " +
      " STORED AS TEXTFILE " +
      " LOCATION '/user/fall2019/marinda/project4/frequencies' " +
      " TBLPROPERTIES ('skip.header.line.count' = '1', 'serialization.null.format' = '')")

    stmt.execute("DROP TABLE IF EXISTS " + dbName + ".enriched_trip")
    stmt.execute("CREATE TABLE IF NOT EXISTS " + dbName + ".enriched_trip ( " +
      " route_id SMALLINT," +
      " service_id STRING, " +
      " trip_id STRING," +
      " trip_headsign STRING," +
      " direction_id TINYINT," +
      " shape_id INT," +
      " note_fr STRING," +
      " note_en STRING, " +
      " date STRING, " +
      " exception_type TINYINT, " +
      " start_time STRING, " +
      " end_time STRING, " +
      " headway_secs SMALLINT) " +
      " PARTITIONED BY (wheelchair_accessible TINYINT) " +
      " STORED AS PARQUET " +
      " TBLPROPERTIES ('parquet.compression' = 'GZIP')")

  }

/*
* An alternative approach will be to not use externall tables:

  def setupStagingArea(dbName: String) = {
    println("Setting up Staging area")
    stmt.execute("SET hive.exec.dynamic.partition.mode=nonstrict")
    stmt.execute("CREATE DATABASE IF NOT EXISTS " + dbName)
    stmt.execute("USE " + dbName)

    stmt.execute("CREATE  TABLE IF NOT EXISTS " + dbName + ".ext_trips ( " +
      " route_id SMALLINT," +
      " service_id STRING, " +
      " trip_id STRING," +
      " trip_headsign STRING," +
      " direction_id TINYINT," +
      " shape_id INT," +
      " wheelchair_accessible TINYINT," +
      " note_fr STRING," +
      " note_en STRING)" +
      " ROW FORMAT DELIMITED " +
      " FIELDS TERMINATED BY ',' " +
      " STORED AS TEXTFILE " +
      " LOCATION '/user/fall2019/marinda/project4/trips' " +
      " TBLPROPERTIES ('skip.header.line.count' = '1', 'serialization.null.format' = '')")

    stmt.execute("CREATE  TABLE IF NOT EXISTS " + dbName + ".ext_calendar_dates ( " +
      " service_id STRING, " +
      " date STRING, " +
      " exception_type TINYINT) " +
      " ROW FORMAT DELIMITED " +
      " FIELDS TERMINATED BY ',' " +
      " STORED AS TEXTFILE " +
      " LOCATION '/user/fall2019/marinda/project4/calendar_dates' " +
      " TBLPROPERTIES ('skip.header.line.count' = '1', 'serialization.null.format' = '')")

    stmt.execute("CREATE  TABLE IF NOT EXISTS " + dbName + ".ext_frequencies ( " +
      " trip_id STRING," +
      " start_time STRING, " +
      " end_time STRING, " +
      " headway_secs SMALLINT) " +
      " ROW FORMAT DELIMITED " +
      " FIELDS TERMINATED BY ',' " +
      " STORED AS TEXTFILE " +
      " LOCATION '/user/fall2019/marinda/project4/frequencies' " +
      " TBLPROPERTIES ('skip.header.line.count' = '1', 'serialization.null.format' = '')")

    stmt.execute("DROP TABLE IF EXISTS " + dbName + ".enriched_trip")
    stmt.execute("CREATE TABLE IF NOT EXISTS " + dbName + ".enriched_trip ( " +
      " route_id SMALLINT," +
      " service_id STRING, " +
      " trip_id STRING," +
      " trip_headsign STRING," +
      " direction_id TINYINT," +
      " shape_id INT," +
      " note_fr STRING," +
      " note_en STRING, " +
      " date STRING, " +
      " exception_type TINYINT, " +
      " start_time STRING, " +
      " end_time STRING, " +
      " headway_secs SMALLINT) " +
      " PARTITIONED BY (wheelchair_accessible TINYINT) " +
      " STORED AS PARQUET " +
      " TBLPROPERTIES ('parquet.compression' = 'GZIP')")

*
     hadoop fs -put /home/marinda/Downloads/trips.csv /user/fall2019/marinda/project4/trips
     hadoop fs -put /home/marinda/Downloads/calendar_dates.csv /user/fall2019/marinda/project4/calendar_dates
     hadoop fs -put /home/marinda/Downloads/frequencies.csv /user/fall2019/marinda/project4/frequencies


*  You won't be able to see the new set of data.
* The reason is that you have updated the underlying data storage but not the Hive metastore
*  and partitions are part of the metadata. In order to get the partitions:

  MSCK REPAIR TABLE your_name.enriched_movie_p;
  SHOW PARTITIONS your_name.enriched_movie_p;
  SELECT * FROM your_name.enriched_movie_p;

  }


 */
  // =================================================================================================================
  def populateStagingArea(lPath: String, fsPath: String):Unit = {

    fs.copyFromLocalFile(new Path(lPath + "/trips.txt"), new Path(fsPath + "/trips"))
    fs.copyFromLocalFile(new Path(lPath + "/calendar_dates.txt"), new Path(fsPath + "/calendar_dates"))
    fs.copyFromLocalFile(new Path(lPath + "/frequencies.txt"), new Path(fsPath + "/frequencies"))
  }
  // =================================================================================================================
  def enrichTrip(dbName: String) = {
    println("Enriching Trip")
    stmt.execute("INSERT INTO " + dbName + ".enriched_trip PARTITION(wheelchair_accessible) " +
      "SELECT t.route_id, " +
      "t.service_id, " +
      "t.trip_id, " +
      "t.trip_headsign, " +
      "t.direction_id, " +
      "t.shape_id, " +
      "t.note_fr, " +
      "t.note_en, " +
      "c.date, " +
      "c.exception_type, " +
      "f.start_time, " +
      "f.end_time, " +
      "f.headway_secs, " +
      "t.wheelchair_accessible " +
      "FROM ext_trips AS t " +
      "LEFT join ext_calendar_dates AS c ON  (t.service_id = c.service_id) " +
      "LEFT join ext_frequencies AS f ON (t.trip_id = f.trip_id)")
  }

}
