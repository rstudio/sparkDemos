### rsparkling hello world
### requires R packages: statmod, RCurl, and devtools

install.packages("h2o", type = "source", repos = "http://h2o-release.s3.amazonaws.com/h2o/rel-turnbull/2/R")
install.packages("rsparkling")

library(rsparkling) 
library(sparklyr)
library(dplyr)
library(h2o)

options(rsparkling.sparklingwater.version = "2.0.3")

conf <- spark_config()
conf$'sparklyr.shell.executor-memory' <- "20g"
conf$'sparklyr.shell.driver-memory' <- "20g"
conf$spark.executor.cores <- 16
conf$spark.executor.memory <- "20G"
conf$spark.yarn.am.cores  <- 16
conf$spark.yarn.am.memory <- "20G"
conf$spark.dynamicAllocation.enabled <- "false"

Sys.setenv(SPARK_HOME="/usr/lib/spark")
sc <- spark_connect(master = "yarn-client", config = conf, version =  "2.0.0")

mtcars_tbl <- copy_to(sc, mtcars, overwrite = TRUE)
mtcars_hf <- as_h2o_frame(sc, mtcars_tbl)

glm_model <- h2o.glm(x = c("wt", "cyl"), 
                     y = "mpg", 
                     training_frame = mtcars_hf,
                     lambda_search = TRUE)
summary(glm_model)

