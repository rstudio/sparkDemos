library(sparklyr)
library(rsparkling)
library(dplyr)
library(ggplot2)

# Set up
sc <- spark_connect("local", version = "1.6.2")
mtcars_tbl <- copy_to(sc, mtcars, "mtcars", overwrite = TRUE)

### Data Prep

# Transform our data set, and then partition into 'training', 'test'
partitions <- mtcars_tbl %>%
  filter(hp >= 100) %>%
  mutate(cyl8 = cyl == 8) %>%
  sdf_partition(training = 0.5, test = 0.5, seed = 1099)

# Convert to H20 Frame
training <- as_h2o_frame(sc, partitions$training)
test <- as_h2o_frame(sc, partitions$test)

### Linear Model

# Fit a linear model to the training dataset
glm_model <- h2o.glm(x = c("wt", "cyl"), 
                     y = "mpg", 
                     training_frame = training,
                     lambda_search = TRUE)
# Examine model
print(glm_model)

### Plot

# Compute predicted values on our test dataset
pred <- h2o.predict(glm_model, newdata = test)

# Convert from H2O Frame to Spark DataFrame
predicted <- as_spark_dataframe(sc, pred)

# Extract the true 'mpg' values from our test dataset
actual <- partitions$test %>%
  select(mpg) %>%
  collect() %>%
  `[[`("mpg")

# Produce a data.frame housing our predicted + actual 'mpg' values
data <- data.frame(
  predicted = predicted,
  actual    = actual
)

# a bug in data.frame does not set colnames properly; reset here 
names(data) <- c("predicted", "actual")

# plot predicted vs. actual values
ggplot(data, aes(x = actual, y = predicted)) +
  geom_abline(lty = "dashed", col = "red") +
  geom_point() +
  theme(plot.title = element_text(hjust = 0.5)) +
  coord_fixed(ratio = 1) +
  labs(
    x = "Actual Fuel Consumption",
    y = "Predicted Fuel Consumption",
    title = "Predicted vs. Actual Fuel Consumption"
  )


