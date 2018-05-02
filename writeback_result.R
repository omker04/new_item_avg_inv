library(dplyr)
library(data.table)
library(magrittr)

source('/home/rstudio/new_functions.R')

project_id <<- getArgument('project_id')
predict_output <- read.csv(paste0('/home/rstudio/predict_output_', project_id, '.csv')) %>% 
  select(mds_fam_id, tot_wkly_qty, avg_price, upspw, store_count, turn, str_inv, predicted_avg_inv, predicted_error)
original_output <- read.csv(paste0('/home/rstudio/original_best_output_', project_id, '.csv')) %>% 
  select(mds_fam_id, tot_wkly_qty, avg_price, upspw, store_count, turn, str_inv, predicted_avg_inv, predicted_error)

file.remove(paste0('/home/rstudio/predict_output_', project_id, '.csv'))
file.remove(paste0('/home/rstudio/original_best_output_', project_id, '.csv'))

predict_error <- (mean(predict_output$predicted_error) + median(predict_output$predicted_error)) / 2 
original_error <- (mean(original_output$predicted_error) + median(original_output$predicted_error)) / 2

connector.execute(name = 'myHiveConnector_new_db', 
                  statement = paste0('drop table if exists new_item_avg_inv.new_item_inv_', project_id, ';')
                  )
connector.execute(name = 'myHiveConnector_new_db', 
                  statement = paste0('create table new_item_avg_inv.new_item_inv_', project_id, 
                                     ' like new_item_avg_inv.new_item_inv_dummy;')
                  )

if (predict_error <= original_error) {
  httr::set_config(httr::config(ssl_verifypeer = 0L))
  df <- predict_output %>% 
    mutate('used_method' = '"Predicted"',
           'mean_pred_error' = predict_error,
           'mean_best_error' = original_error
    )
} else {
  httr::set_config(httr::config(ssl_verifypeer = 0L))
  df <- original_output %>% 
    mutate('used_method' = '"Best"',
           'mean_pred_error' = predict_error,
           'mean_best_error' = original_error
    )
}

val <- sapply(1:nrow(df), function(x) {
  paste(df[x,], collapse = ', ')
}) %>% paste(., collapse = ' union all select ')

if (nrow(df) == 1) {
  connector.execute(name = 'myHiveConnector_new_db', 
                    statement = paste0('insert into table new_item_avg_inv.new_item_inv_', project_id, 
                                       ' values (', val, ');')
                    )
} else {
  connector.execute(name = 'myHiveConnector_new_db', 
                    statement = paste0('insert into table new_item_avg_inv.new_item_inv_', project_id, 
                                       ' select ', val, ';')
  )
}
