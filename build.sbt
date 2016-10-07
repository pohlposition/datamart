name := "datamart"

version := "1.0"

scalaVersion := "2.11.8"

organization := "com.databricks.blog"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-sql_2.10" % "2.0.0" % "provided")

// Your username to login to Databricks Cloud
dbcUsername := System.getenv("DBC_USERNAME")

// Your password (Can be set as an environment variable)
dbcPassword := System.getenv("DBC_PASSWORD")

// The URL to the Databricks Cloud DB Api.!
dbcApiUrl := System.getenv("DBC_URL") + "/api/1.2"

// Add any clusters that you would like to deploy your work to. e.g. "My Cluster"
// or run dbcExecuteCommand
// Add "ALL_CLUSTERS" if you want to attach your work to all clusters
dbcClusters += System.getenv("DBC_CLUSTER")

// The location to upload your libraries to in the workspace e.g. "/Users/alice"
dbcLibraryPath := "/Users/" + System.getenv("DBC_USERNAME") + "/lib"
