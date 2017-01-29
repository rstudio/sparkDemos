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

library(devtools)
devtools::install_github("h2oai/rsparkling", ref = "stable")

### 

library(sparklyr)
spark_install(version = "1.6.2")
options(rsparkling.sparklingwater.version = "1.6.2") # Use Sparkling Water 1.6.2 to match Spark version

library(sparklyr)
library(rsparkling)
library(dplyr)

Sys.setenv(SPARK_HOME="/usr/lib/spark")
config <- spark_config()
config$spark.dynamicAllocation.enabled <- "false"
options(rsparkling.sparklingwater.version = '1.6.7')
sc <- spark_connect(master = "yarn-client", config = config, version = '1.6.2')
airlines_tbl <- tbl(sc, "airlines")
h2oframe <- as_h2o_frame(sc, airlines_tbl)


mtcars_tbl <- copy_to(sc, mtcars, "mtcars", overwrite = TRUE)

partitions <- mtcars_tbl %>%
  filter(hp >= 100) %>%
  mutate(cyl8 = cyl == 8) %>%
  sdf_partition(training = 0.5, test = 0.5, seed = 1099)

training <- as_h2o_frame(sc, partitions$training)
test <- as_h2o_frame(sc, partitions$test)

install.packages("h2o", type="source", repos=(c("http://h2o-release.s3.amazonaws.com/h2o/rel-turing/6/R")))
library(h2o)
localH2O = h2o.init(nthreads=-1)


