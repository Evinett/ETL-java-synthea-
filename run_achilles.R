# This is an R script for running the OHDSI Achilles data characterization tool.
# It uses the same database connection settings as your Java ETL project.

# --- Package Management ---
# This section ensures all required packages are installed. It is designed to be
# robust, especially on macOS where compilation can be an issue.

message("--- Checking and Installing R Package Dependencies ---")

# Helper function to check and install a package from CRAN
install_if_missing <- function(pkg, repo = "http://cran.us.r-project.org") {
  if (!require(pkg, character.only = TRUE)) {
    message(paste("Installing CRAN package:", pkg))
    install.packages(pkg, repos = repo)
  }
}

# Helper function to check and install a GitHub package
install_github_if_missing <- function(pkg_name, github_repo) {
  # We need remotes for this, so ensure it's installed first.
  install_if_missing("remotes")
  if (!require(pkg_name, character.only = TRUE)) {
    message(paste("Installing GitHub package:", pkg_name, "from", github_repo))
    remotes::install_github(github_repo)
  }
}

# 1. Install all required CRAN packages.
cran_packages <- c(
  "rJava", "dplyr", "jsonlite", "readr", "data.table", "lubridate",
  "tseries", "rlang", "stringr", "urltools", "bit64", "checkmate", "digest",
  "dbplyr", "rstudioapi", "getPass"
)
for (pkg in cran_packages) {
  install_if_missing(pkg)
}

# 2. Install OHDSI packages from GitHub in the correct dependency order.
install_github_if_missing("ParallelLogger", "OHDSI/ParallelLogger")
install_github_if_missing("SqlRender", "OHDSI/SqlRender")
install_github_if_missing("DatabaseConnector", "OHDSI/DatabaseConnector")
install_github_if_missing("Achilles", "OHDSI/Achilles")

message("--- All package dependencies have been checked. ---")

# --- Load Libraries ---
message("Loading required R packages...")

library(DatabaseConnector)
library(Achilles)
library(getPass)

# --- Connection Details ---
# These details are based on your ETL-Java project's DatabaseManager.java
dbms <- "postgresql"
user <- "rward"
message("Please enter your database password.")
password <- getPass(msg = "Password: ")
server <- "localhost/Syn_CDM" # For PostgreSQL, the format is "host/database"
port <- 5432
pathToDriver <- "/Users/rward/jdbc_drivers"  # Folder containing postgresql-42.7.7.jar

# Create the connectionDetails object that Achilles will use
connectionDetails <- createConnectionDetails(
  dbms = dbms,
  server = server,
  user = user,
  password = password,
  port = port,
  pathToDriver = pathToDriver
)

# --- Schema Definitions ---
cdmDatabaseSchema <- "cdm_synthea"
resultsDatabaseSchema <- "results_synthea" # Separate schema for Achilles results

# --- Achilles Execution ---
# Before running, make sure the results schema exists in your database:
# CREATE SCHEMA IF NOT EXISTS results_synthea;

message("--- Starting Achilles Analysis ---")
message("This process can take a long time to complete.")

tryCatch({
  achilles(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    resultsDatabaseSchema = resultsDatabaseSchema,
    cdmVersion = "5.4", # As specified in EtlRunner.java
    outputFolder = "achilles_output",
    verboseMode = TRUE # Added for detailed progress logging
  )
  message("\nSUCCESS: Achilles analysis complete. Check the 'achilles_output' folder for results.")

}, error = function(e) {
  message("\nERROR: The Achilles analysis failed.")
  message("Here is the full error message from R:")
  message(e)

  # Provide specific advice for common runtime errors
  if (grepl("authentication failed", e$message, ignore.case = TRUE)) {
    message("\nHint: The error suggests a problem with your database password. Please check it and try again.")
  } else if (grepl("database .* does not exist", e$message, ignore.case = TRUE)) {
    message("\nHint: The database 'Syn_CDM' was not found. Is PostgreSQL running and is the database created?")
  } else if (grepl("schema .* does not exist", e$message, ignore.case = TRUE)) {
    message("\nHint: A required schema (e.g., 'cdm_synthea' or 'results_synthea') was not found. Please ensure they exist before running.")
  }
})