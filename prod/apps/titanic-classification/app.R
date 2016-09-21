library(sparklyr)
library(dplyr)
library(shiny)
library(ggplot2)
library(tidyr)
source('helpers.R')


#Connect to Spark
sc <- spark_connect(master = "local", version = "2.0.0")

#Read in Parquet Data
spark_read_parquet(sc, "titanic", "titanic-parquet")
titanic_tbl <- tbl(sc, "titanic")

# Add features
titanic_final <- titanic_tbl %>% 
  mutate(Family_Size = SibSp + Parch + 1L) %>% 
  mutate(Pclass = as.character(Pclass)) %>%
  filter(!is.na(Embarked)) %>%
  mutate(Age = if_else(is.na(Age), mean(Age), Age)) %>%
  mutate(Family_Size = as.numeric(Family_size)) %>%
  sdf_mutate(
    Family_Sizes = ft_bucketizer(Family_Size, splits = c(1,2,5,12))
  ) %>%
  mutate(Family_Sizes = as.character(as.integer(Family_Sizes))) %>%
  mutate(Survived = as.numeric(Survived), SibSp = as.numeric(SibSp), Parch = as.numeric(Parch)) %>%
  select(Survived, Pclass, Sex, Age, SibSp, Parch, Fare, Embarked, Family_Sizes) %>% 
  sdf_register("titanic_final")

features <- tbl_vars(titanic_final) %>%
  .[-which(. == "Survived")]


ui <- pageWithSidebar(
  headerPanel('ML Titanic Classification'),
  sidebarPanel(
    selectizeInput('selfeatures', 'Select Features', features, multiple = TRUE),
    numericInput('trainingFrac', 'Training Proportion', min = 0.1, max = 0.9, value = 0.75), 
    actionButton('fit', "Fit Models")
  ),
  mainPanel(
    plotOutput('liftPlot'),
    plotOutput('auc_accuracy')
  )
)

server <- function(input, output, session) {
  
  ml_score <- eventReactive(input$fit, {
    withProgress(message = "Fitting Spark Models", value = 0.1, {
        incProgress(0.2, detail = "Partitioning Training / Testing")
        partition <- sdf_partition(titanic_final, train = input$trainingFrac, test= 1-input$trainingFrac)
        train_tbl <- partition$train
        test_tbl <- partition$test
        
        ml_formula <- formula(paste("Survived ~", paste(input$selfeatures, collapse = "+")))
        
        incProgress(0.5, detail = "Fitting Models")
        ml_models <- list(
          "Logistic" = ml_logistic_regression(train_tbl, ml_formula), 
          "Decision Tree" = ml_decision_tree(train_tbl, ml_formula),
          "Random Forest" = ml_random_forest(train_tbl, ml_formula),
          "Gradient Boosted Trees" = ml_gradient_boosted_trees(train_tbl, ml_formula),
          "Naive Bayes" = ml_naive_bayes(train_tbl, ml_formula)
        )
        
        incProgress(0.75, detail = "Scoring Models")
        lapply(ml_models, score_test_data, test_tbl) # helpers.R
    })
  })
  
  output$liftPlot <- renderPlot({
    
      ml_gains <- data.frame(bin = 1:10, prop = seq(0, 1, len = 10), model = "Base")
      for (i in names(ml_score())) {
        ml_gains <- ml_score()[[i]] %>%
          calculate_lift %>% # helpers.R 
          mutate(model = i) %>%
          rbind(ml_gains, .)
      }
      ggplot(ml_gains, aes(x = bin, y = prop, colour = model)) +
        geom_point() + geom_line() +
        ggtitle("Lift Chart for Predicting Survival - Test Data Set") + 
        xlab("") + ylab("")
    
  })
  
  output$auc_accuracy <- renderPlot({
    # Calculate AUC and accuracy
    perf_metrics <- data.frame(
      model = names(ml_score()),
      AUC = 100 * sapply(ml_score(), ml_binary_classification_eval, "Survived", "prediction"),
      Accuracy = 100 * sapply(ml_score(), calc_accuracy),
      row.names = NULL, stringsAsFactors = FALSE)
    
    # Plot results
    gather(perf_metrics, metric, value, AUC, Accuracy) %>%
      ggplot(aes(reorder(model, value), value, fill = metric)) + 
      geom_bar(stat = "identity", position = "dodge") + 
      coord_flip() +
      xlab("") +
      ylab("Percent") +
      ggtitle("Performance Metrics")
    
  })
  
}

shinyApp(ui = ui, server = server)
