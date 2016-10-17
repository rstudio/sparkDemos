library(devtools)
library(sparklyr)

# Remove previous versions of h2o R package
if ("package:h2o" %in% search()) detach("package:h2o", unload=TRUE)
if ("h2o" %in% rownames(installed.packages())) remove.packages("h2o")

# Next, we download R package dependencies
pkgs <- c("methods","statmod","stats","graphics",
          "RCurl","jsonlite","tools","utils")
for (pkg in pkgs) {
  if (!(pkg %in% rownames(installed.packages()))) install.packages(pkg)
}

# Download h2o package version 3.10.0.6
install.packages("h2o", type = "source", 
                 repos = "http://h2o-release.s3.amazonaws.com/h2o/rel-turing/6/R")

# Install from github
devtools::install_github("h2oai/sparkling-water", subdir = "/r/rsparkling")

# Make sure spark is also installed in local mode
spark_install(version = "1.6.2")
