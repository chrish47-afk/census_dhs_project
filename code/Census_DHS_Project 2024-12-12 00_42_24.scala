//////////////////////////////////
//////// Sourcing in Data ////////
//////////////////////////////////
import spark.implicits._
import org.apache.spark.sql.SparkSession

//////// Measure Start Time ////////
val startTime = System.nanoTime() //nano is more precised than milli.

// Reading in Data Files
val filepath_census = abfssBasePath + "Project_Data_Census_DHS/acs5_immigration_foreign_allyears_final.csv"
val filepath_dhs2 = abfssBasePath + "Project_Data_Census_DHS/DHS_table2_lawful_permanent_resident.csv"

val census = spark.read
                  .option("header", "true") // Use the first row as column names
                  .csv(filepath_census)
val dhslawful = spark.read
                .option("header", "true")
                .csv(filepath_dhs2)
//census.schema

// COMMAND ----------

//////////////////////////////////
//// Data Review - Validation ////
//////////////////////////////////
import spark.implicits._
import org.apache.spark.sql.SparkSession

//////// Measure Start Time ////////
val startTime = System.nanoTime() //nano is more precised than milli.

//////// Unique Conditions - Validation ////////
// Number of columns
val numColumns = census.columns.length
println(s"Number of census df columns: $numColumns")

// Number of rows
val numRows = census.count()
println(s"Number of census df rows: $numRows")

// Show unique values of a specific column (e.g., "name")
val uniquestatename = census.select("state_name") //This should equal 50 states, 50 US states.
                          .distinct()
                          .count()
val uniquecountyname = census.select("county_name")
                              .distinct()
                              .count()
val uniquevariableE = census.columns.filter(_.endsWith("E"))
                                    .length
val uniquevariableM = census.columns.filter(_.endsWith("M"))
                                    .length

println(s"Census df - Number of unique States: $uniquestatename")
println(s"Census df - Number of unique Counties: $uniquecountyname")
println(s"Census df - Number of unique column variables that end with E: $uniquevariableE")
println(s"Census df - Number of unique column variables that end with M: $uniquevariableM")
//uniquestatename.show()

//////// Schema - Data Types ////////
// Get the schema of the DataFrame
val datatypescensus = census.schema.fields.map(_.dataType)
                                    .distinct
val datatypeslawful = dhslawful.schema.fields.map(_.dataType)
                                    .distinct

// Print the unique data types
println("Unique data types in census df:")
datatypescensus.foreach(println)
println("Unique data types in dhslawful df:")
datatypeslawful.foreach(println)

//////// Measure End Time ////////
val endTime = System.nanoTime()

// Calculate and print execution time in seconds
val duration = (endTime - startTime) / 1e9d
println(s"Code execution took $duration seconds.")

// COMMAND ----------

/////////////////////////////////
//// Data Questions - Manual ////
/////////////////////////////////
import spark.implicits._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._ // For aggregation functions

val censuspop = census.select(
                              $"year",
                              $"state_name",
                              $"county_name",
                              $"B05001_001E",
                              $"B05002_001E"
                              )
                    .filter($"year" === 2022)
                    .filter($"state_name" === "Washington")
////////Census - Calculating the sum of B05001_001E at different levels////////
val totalcensuspop = census.filter($"year" === 2022)
                          .agg(sum($"B05001_001E").alias("total_us_population"))
                          .collect()(0)(0)
val totalpop = censuspop.agg(sum($"B05001_001E").alias("total_state_population"))
                        .collect()(0)(0) // Collecting the sum as a scalar value
val foreignpop = censuspop.agg(sum($"B05002_001E").alias("total_state_foreign_population"))
                        .collect()(0)(0) // Collecting the sum as a scalar value
val totalpop_bycounty = censuspop.filter($"county_name".contains("King County"))
                                .groupBy($"county_name")
                                .agg(sum($"B05001_001E").alias("total_county_population"))
                                .collect()(0)(1) // Collecting the sum as a scalar value

println(s"Total US Population in 2022: $totalcensuspop")
println(s"Total Population for Washington in 2022: $totalpop")
println(s"Total Foreign Population for Washington in 2022: $foreignpop")
println(s"Total Population for Washington, Clark County in 2022: $totalpop_bycounty")

////////DHS - Wrangling for different level and country level data & agggregating as necessary////////
/**
Some of the DHS data just doesn't make sense to me. Including the exclusion of some countries, in particularly central and southern american countries.
For that areason. Only Table 13, 17, and 19 are explored. There are other tables that can be further explored, but due to time limitations, I focused on the refugee section.
**/
val dhslawfula = dhslawful.filter($"Region and country of last residence" === "Mexico")
                          .select($"2014")
                          .collect()(0)(0)

println(s"Persons obtaining lawful permanent resident status by region and selected country of last residence: $dhslawfula")

// COMMAND ----------

/////////////////////////////////
//// Data Questions - Manual, excluding Total(Draft) ////
/////////////////////////////////
import spark.implicits._
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._ // For aggregation functions

/**
//////// Master Function ////////
def aggregateCensusData(census: DataFrame, 
                        year: Int, 
                        stateName: String, 
                        countyName: Option[String] = None, 
                        variables: Seq[String]): Map[String, Any] = {

  // Select relevant columns dynamically based on input variables
  val selectedColumns = Seq($"year", $"state_name", $"county_name") ++ variables.map(census.col)
  val filteredCensus = census.select(selectedColumns: _*)
                             .filter($"year" === year)
                             .filter($"state_name" === stateName)

  // Handle aggregation logic based on countyName input
  val result = countyName match {
    case Some(county) => 
      // Aggregate at the county level
      val grouped = filteredCensus.filter($"county_name".contains(county))
                                  .groupBy($"county_name")
      
      val aggExprs = variables.map(v => sum(census.col(v)).alias(s"${v}_total"))
      grouped.agg(aggExprs.head, aggExprs.tail: _*)
             .collect()
             .map(row => row.getValuesMap[Any](row.schema.fieldNames))
             .headOption.getOrElse(Map("Error" -> "No data found for the specified county"))
      
    case None => 
      // Construct aggregation expressions
      val aggExprs = variables.map(v => sum(census.col(v)).alias(s"${v}_total"))
      // Aggregate at the state level
      filteredCensus.agg(aggExprs.head, aggExprs.tail: _*)
                    .collect()(0)
                    .getValuesMap[Any](variables.map(v => s"${v}_total"))
  }

  result
}

//////// Example Calls ////////
val resultState = aggregateCensusData(
  census = census,
  year = 2022,
  stateName = "Oregon",
  variables = Seq("B05001_001E", "B05002_001E")
)
println(resultState)

val resultCounty = aggregateCensusData(
  census = census,
  year = 2022,
  stateName = "Washington",
  countyName = Some("King County"),
  variables = Seq("B05001_001E", "B05002_001E")
)
println(resultCounty)
**/

// COMMAND ----------

////////////////////////////////////////
//// Data Questions - Main Function ////
////////////////////////////////////////
import spark.implicits._
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._ // For aggregation functions

//////// Master Function ////////
def aggregateCensusData(census: DataFrame, 
                        year: Int, 
                        stateName: Option[String] = None, 
                        countyName: Option[String] = None, 
                        variables: Seq[String]): Map[String, Any] = {

  // Select relevant columns dynamically based on input variables
  val selectedColumns = Seq($"year", $"state_name", $"county_name") ++ variables.map(census.col)
  val filteredCensus = census.select(selectedColumns: _*)
                             .filter($"year" === year)

  // Handle aggregation logic based on stateName and countyName inputs
  val result = (stateName, countyName) match {
    case (Some(state), Some(county)) => 
      // Aggregate at the county level
      val grouped = filteredCensus.filter($"state_name" === state && $"county_name".contains(county))
                                  .groupBy($"county_name")
      
      val aggExprs = variables.map(v => sum(census.col(v)).alias(s"${v}_total"))
      grouped.agg(aggExprs.head, aggExprs.tail: _*)
             .collect()
             .map(row => row.getValuesMap[Any](row.schema.fieldNames))
             .headOption.getOrElse(Map("Error" -> "No data found for the specified county"))

    case (Some(state), None) => 
      // Aggregate at the state level
      val stateFiltered = filteredCensus.filter($"state_name" === state)
      val aggExprs = variables.map(v => sum(census.col(v)).alias(s"${v}_total"))
      stateFiltered.agg(aggExprs.head, aggExprs.tail: _*)
                   .collect()(0)
                   .getValuesMap[Any](variables.map(v => s"${v}_total")) //I can also get rid of this line. Review.

    case (None, None) => 
      // Aggregate at the national (US) level
      val aggExprs = variables.map(v => sum(census.col(v)).alias(s"${v}_total"))
      filteredCensus.agg(aggExprs.head, aggExprs.tail: _*)
                    .collect()(0)
                    .getValuesMap[Any](variables.map(v => s"${v}_total")) //I can also get rid of this line. Review.

    case _ => 
      Map("Error" -> "Invalid input: Provide either a state name or none for US-level aggregation.")
  }

  result
}

//////// Example Calls ////////
val resultUS = aggregateCensusData(
  census = census,
  year = 2022,
  variables = Seq("B05001_006E")
)
println(s"US-level aggregation: $resultUS")

val resultState = aggregateCensusData(
  census = census,
  year = 2022,
  stateName = Some("Oregon"),
  variables = Seq("B05001_001E", "B05001_006E")
)
println(s"State-level aggregation: $resultState")

val resultCounty = aggregateCensusData(
  census = census,
  year = 2022,
  stateName = Some("Washington"),
  countyName = Some("King County"),
  variables = Seq("B05001_001E", "B05001_006E")
)
println(s"County-level aggregation: $resultCounty")


// COMMAND ----------

import spark.implicits._
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._ // For aggregation functions

//////// Function for Lawful Resident Count ////////
def getLawfulResidentCount(dhs: DataFrame, 
                           year: String, 
                           country: Option[String] = None): Any = {

  // Select relevant columns dynamically (Region/Country and Year)
  val selectedColumns = Seq($"Region and country of last residence", dhs.col(year))
  val filteredDHS = dhs.select(selectedColumns: _*)

  // Handle logic based on country input
  val result = country match {
    case Some(countryName) =>
      // Get the count for a specific country
      filteredDHS.filter($"Region and country of last residence" === countryName)
                 .select(year)
                 .collect()(0)
                 .getValuesMap[Any](Seq(year)) //I can also get rid of this line. Review.
    case None =>
      // Default to "Global" or return a message
      s"Please provide a country name to retrieve the count."
  }

  result
}

// Example 1: Get count for a specific country and year
val resultMexico = getLawfulResidentCount(
  dhs = dhslawful, 
  year = "2022", 
  country = Some("Mexico")
)
println(s"Persons obtaining lawful permanent resident status by region or selected country of last residence: $resultMexico")
