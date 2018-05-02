library(dtplyr)
library(dplyr)
library(data.table)
library(magrittr)
library(cluster)
library(FastKNN)
library(ggplot2)
library(rpart)
library(partykit)
library(mlutils)

# source('/home/rstudio/new_functions.R')
master_df <- dataset.load(name = 'myHiveConnector_new_db', query = 'select * from new_item_avg_inv.master_df;') %>% 
  unique()
existing_details <- dataset.load(name = 'project_details') %>% 
  mutate(project = project_id) %>% 
  unique()

project_id <<- getArgument('project_id') %>% as.numeric()
mod_drop_wk <<- getArgument('mod_drop_wk') %>% as.numeric()

end_wk_nbr <- (substr(mod_drop_wk, 5, 7) %>% as.numeric() + 26) %% 52
if (end_wk_nbr < 26) {
  if (end_wk_nbr < 10) {
    end_wk_nbr <- paste0('0', end_wk_nbr)
  }
  yr <- (substr(mod_drop_wk, 1, 4) %>% as.numeric() + 1)
} else {
  yr <- substr(mod_drop_wk, 1, 4) %>% as.numeric()
}
end_wk <<- paste0(yr, end_wk_nbr) %>% as.numeric()

n2 <<- 35
k <<- 20
N <<- 100
weight <<- 'reciprocal'
k_type <<- 'percentile'

source('/home/rstudio/project_details.R')

new_proj_details <- get(paste0('project_details_', project_id))
dataset.save(df = rbind(existing_details, 
                        new_proj_details %>% 
                          mutate(project = project_id)
                        ) %>% 
               unique(), 
             name = 'project_details', over_write = TRUE)


project_details_best_pass <- master_df %>% group_by(project, method, target_type) %>% summarise(m = mean(c(mean, median)))
project_details_best_pass <- inner_join(project_details_best_pass, 
                                        project_details_best_pass %>% 
                                          group_by(project) %>% 
                                          summarise(m = min(m))
) %>% 
  ungroup() %>% 
  mutate(aggregation = if_else(method %like% 'reg', 'linear_regression', 'weighted_mean'),
         method = if_else(method %like% 're_sim', '2nd_stage_similarity', 
                          if_else(method %like% 're_class', '2nd_stage_classification', 'simple'))
  ) %>% 
  inner_join(., existing_details) %>% 
  select(-project_id) %>% 
  unique()

pred_method <- rpart(method ~ ., data = project_details_best_pass %>% 
                       select(-c(aggregation, target_type)) %>% unique() %>% select(-project, -m), 
                     control = rpart.control(minsplit = 1, cp = 0))
pred_agg <- rpart(aggregation ~ ., data = project_details_best_pass %>% 
                    select(-c(method, target_type)) %>% unique() %>% select(-project, -m), 
                  control = rpart.control(minsplit = 1, cp = 0))
pred_target <- rpart(target_type ~ ., data = project_details_best_pass %>% 
                       select(-c(aggregation, method)) %>% unique() %>% select(-project, -m), 
                     control = rpart.control(minsplit = 1, cp = 0))

method_to_follow <- pred_method %>% 
  predict(., new_proj_details) %>% 
  t() %>% as.data.frame() %>% 
  mutate(method = rownames(.)) %>% 
  set_colnames(c('pred', 'method')) %>% 
  filter(pred > 0)

agg_to_follow <- pred_agg %>% 
  predict(., new_proj_details) %>% 
  t() %>% as.data.frame() %>% 
  mutate(agg = rownames(.)) %>% 
  set_colnames(c('pred', 'agg'))

target_to_follow <- pred_target %>% 
  predict(., new_proj_details) %>% 
  t() %>% as.data.frame() %>% 
  mutate(target = rownames(.)) %>% 
  set_colnames(c('pred', 'target')) %>% 
  filter(pred > 0)


run_now <- function(proj, mod_drop, target, method, agg) {
  project_id <<- as.numeric(proj)
  mod_drop_wk <<- as.numeric(mod_drop)
  end_wk_nbr <- (substr(mod_drop_wk, 5, 7) %>% as.numeric() + 26) %% 52
  if (end_wk_nbr < 26) {
    if (end_wk_nbr < 10) {
      end_wk_nbr <- paste0('0', end_wk_nbr)
    }
    yr <- (substr(mod_drop_wk, 1, 4) %>% as.numeric() + 1)
  } else {
    yr <- substr(mod_drop_wk, 1, 4) %>% as.numeric()
  }
  end_wk <<- paste0(yr, end_wk_nbr) %>% as.numeric()
  
  n2 <<- 35
  k <<- 20
  N <<- 200
  weight <<- 'reciprocal'
  k_type <<- 'percentile'
  sim_method <<- 'sample'
  inv <<- target
  
  source('/home/rstudio/user_defined_functions.R')
  source('/home/rstudio/data_prep.R')
  
  t <- list()
  if ('simple' %in% method) {
    source('/home/rstudio/new_items.R')
    t[['simple']] <- new_item_inv_data
    mean_p <- agg$pred[agg$agg == 'weighted_mean']
    reg_p <- 1 - mean_p
    t[['simple']] %<>% mutate(predicted_avg_inv = mean_p * predicted_avg_inv2 + reg_p * predicted_avg_inv1,
                              predicted_error = abs(str_inv - predicted_avg_inv) / (str_inv + predicted_avg_inv)
                              )
  }
  
  if('2nd_stage_similarity' %in% method) {
    source('/home/rstudio/new_items_repredict.R')
    t[['2nd_stage_similarity']] <- new_item_inv_data
    mean_p <- agg$pred[agg$agg == 'weighted_mean']
    reg_p <- 1 - mean_p
    t[['2nd_stage_similarity']] %<>% mutate(predicted_avg_inv = mean_p * predicted_avg_inv2 + reg_p * predicted_avg_inv1,
                              predicted_error = abs(str_inv - predicted_avg_inv) / (str_inv + predicted_avg_inv)
    )
  }
  
  if('2nd_stage_classification' %in% method) {
    source('/home/rstudio/reconsider_new.R')
    t[['2nd_stage_classification']] <- new_item_inv_data
    mean_p <- agg$pred[agg$agg == 'weighted_mean']
    reg_p <- 1 - mean_p
    t[['2nd_stage_classification']] %<>% mutate(predicted_avg_inv = mean_p * predicted_avg_inv2 + reg_p * predicted_avg_inv1,
                              predicted_error = abs(str_inv - predicted_avg_inv) / (str_inv + predicted_avg_inv)
    )
  }
  return(t)
}

if ('inverse turn' %in% target_to_follow$target) {
  inv_turn_pred <- run_now(proj = project_id, mod_drop = mod_drop_wk, target = 'rec', method = method_to_follow$method, agg = agg_to_follow)
  if (nrow(method_to_follow) > 1) {
    inv <- array(0, nrow(inv_turn_pred[[1]]))
    for(i in 1:nrow(method_to_follow)) {
      inv <- inv + inv_turn_pred[[i]]$predicted_avg_inv * method_to_follow$pred[i]
    }
    inv_turn_pred <- inv_turn_pred[[1]] %>% 
      select(mds_fam_id, tot_wkly_qty, avg_price, upspw, store_count, data_type, str_inv, ipspw, turn) %>% 
      mutate(predicted_avg_inv = inv, 
             predicted_error = abs(predicted_avg_inv - str_inv)/(predicted_avg_inv + str_inv))
  } else {
    inv_turn_pred <- inv_turn_pred[[1]]
  }
}
if ('turn' %in% target_to_follow$target) {
  turn_pred <- run_now(proj = project_id, mod_drop = mod_drop_wk, target = 'rec1', method = method_to_follow$method, agg = agg_to_follow)
  if (nrow(method_to_follow) > 1) {
    inv <- array(0, nrow(turn_pred[[1]]))
    for(i in 1:nrow(method_to_follow)) {
      inv <- inv + turn_pred[[i]]$predicted_avg_inv * method_to_follow$pred[i]
    }
    turn_pred <- turn_pred[[1]] %>% 
      select(mds_fam_id, tot_wkly_qty, avg_price, upspw, store_count, data_type, str_inv, ipspw, turn) %>% 
      mutate(predicted_avg_inv = inv, 
             predicted_error = abs(predicted_avg_inv - str_inv)/(predicted_avg_inv + str_inv))
  } else {
    turn_pred <- turn_pred[[1]]
  }
}

if (nrow(target_to_follow) > 1) {
  final_output <- turn_pred %>% 
    select(mds_fam_id, tot_wkly_qty, avg_price, upspw, store_count, data_type, str_inv, ipspw, turn) %>% 
    mutate(predicted_avg_inv = inv_turn_pred$predicted_avg_inv * target_to_follow$pred[target_to_follow$target == 'inverse turn'] +
             turn_pred$predicted_avg_inv * target_to_follow$pred[target_to_follow$target == 'turn'],
           predicted_error = abs(predicted_avg_inv - str_inv)/(predicted_avg_inv + str_inv))
} else {
  if (target_to_follow$target == 'turn') {
    final_output <- turn_pred
  } else {
    final_output <- inv_turn_pred
  }
}

write.csv(x = final_output, 
          file = paste0('/home/rstudio/predict_output_', project_id, '.csv'), 
          row.names = FALSE)


