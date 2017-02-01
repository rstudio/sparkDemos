### rsparkling hello world
### requires R packages: statmod, RCurl, and devtools

### Install

# install h2o version 3.10.0.6
install.packages("h2o", type = "source", 
                 repos = "http://h2o-release.s3.amazonaws.com/h2o/rel-turing/6/R")

# install rspakrling version 1.6.7
devtools::install_github("h2oai/rsparkling", ref = "stable")

# install spark version 1.6.0
spark_install(version = "1.6.0") # for local

### Run

library(sparklyr)
library(rsparkling)
library(dplyr)

Sys.setenv(SPARK_HOME="/usr/lib/spark")
options(rsparkling.sparklingwater.version = '1.6.7')

sc <- spark_connect(master = "yarn-client", config = conf, version = '1.6.0')

