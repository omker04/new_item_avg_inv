library(dtplyr)
library(dplyr)
library(data.table)
library(magrittr)
library(cluster)
library(FastKNN)
library(ggplot2)
library(rpart)
library(mlutils)
library(partykit)


rm(list = ls())

master_df <- data.frame(
  'project_id' = 1,
  'mod_drop_wk' = 1,
  'target_type' = 1,
  'method' = 1,
  'mean' = 1,
  'median' = 1,
  'var' = 1
) %>% filter(project_id > 1)

project_details <- data.frame(
  'project_id' = -1,
  'nbr_items_in_proj' = 1,
  'nbr_new_items' = 1,
  'nbr_items_per_brand' = 1,
  'med_nbr_items_per_brand' = 1,
  'var_nbr_items_per_brand' = 1,
  'nbr_items_per_fineline' = 1,
  'med_nbr_items_per_fineline' = 1,
  'var_nbr_items_per_fineline' = 1,
  'items_per_dollar' = 1,
  'var_items_per_dollar' = 1,
  'store_count_per_item' = 1,
  'var_store_count_per_item' = 1,
  'wkly_qty' = 1,
  'med_wkly_qty' = 1,
  'var_wkly_qty' = 1,
  'nbr_items_inv_avbl' = 1,
  'inv_per_item' = 1,
  'med_inv_per_item' = 1,
  'var_inv_per_item' = 1,
  'turn' = 1,
  'var_turn' = 1,
  'inv_to_presence' = 1
) %>% filter(project_id > 0)


dataset.save(df = project_details, name = 'project_details')

run <- function(proj, mod_drop) {
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
  inv <<- 'rec'
  
  # print(list.files())
  
  source('/home/rstudio/user_defined_functions.R')
  source('/home/rstudio/data_prep.R')
  
  source('/home/rstudio/new_items.R')
  a <- describe(new_item_inv_data$inv_rel_diff1)
  master_df %<>% 
    rbind(., data.frame(
      'project_id' = project_id,
      'mod_drop_wk' = mod_drop_wk,
      'target_type' = 'inverse turn',
      'method' = 'simple_reg',
      'mean' = a['Mean'],
      'median' = a['Median'],
      'var' = a['Variance']
    ))
  a <- describe(new_item_inv_data$inv_rel_diff2)
  master_df %<>% 
    rbind(., data.frame(
      'project_id' = project_id,
      'mod_drop_wk' = mod_drop_wk,
      'target_type' = 'inverse turn',
      'method' = 'simple_mean',
      'mean' = a['Mean'],
      'median' = a['Median'],
      'var' = a['Variance']
    ))
  
  source('/home/rstudio/new_items_repredict.R')
  a <- describe(new_item_inv_data$inv_rel_diff1)
  master_df %<>% 
    rbind(., data.frame(
      'project_id' = project_id,
      'mod_drop_wk' = mod_drop_wk,
      'target_type' = 'inverse turn',
      'method' = 're_sim_reg',
      'mean' = a['Mean'],
      'median' = a['Median'],
      'var' = a['Variance']
    ))
  a <- describe(new_item_inv_data$inv_rel_diff2)
  master_df %<>% 
    rbind(., data.frame(
      'project_id' = project_id,
      'mod_drop_wk' = mod_drop_wk,
      'target_type' = 'inverse turn',
      'method' = 're_sim_mean',
      'mean' = a['Mean'],
      'median' = a['Median'],
      'var' = a['Variance']
    ))
  
  source('/home/rstudio/reconsider_new.R')
  a <- describe(new_item_inv_data$inv_rel_diff1)
  master_df %<>% 
    rbind(., data.frame(
      'project_id' = project_id,
      'mod_drop_wk' = mod_drop_wk,
      'target_type' = 'inverse turn',
      'method' = 're_class_reg',
      'mean' = a['Mean'],
      'median' = a['Median'],
      'var' = a['Variance']
    ))
  a <- describe(new_item_inv_data$inv_rel_diff2)
  master_df %<>% 
    rbind(., data.frame(
      'project_id' = project_id,
      'mod_drop_wk' = mod_drop_wk,
      'target_type' = 'inverse turn',
      'method' = 're_class_mean',
      'mean' = a['Mean'],
      'median' = a['Median'],
      'var' = a['Variance']
    ))
  
  inv <<- 'rec1'
  
  source('/home/rstudio/user_defined_functions.R')
  source('/home/rstudio/data_prep.R')
  
  source('/home/rstudio/new_items.R')
  a <- describe(new_item_inv_data$inv_rel_diff1)
  master_df %<>% 
    rbind(., data.frame(
      'project_id' = project_id,
      'mod_drop_wk' = mod_drop_wk,
      'target_type' = 'turn',
      'method' = 'simple_reg',
      'mean' = a['Mean'],
      'median' = a['Median'],
      'var' = a['Variance']
    ))
  a <- describe(new_item_inv_data$inv_rel_diff2)
  master_df %<>% 
    rbind(., data.frame(
      'project_id' = project_id,
      'mod_drop_wk' = mod_drop_wk,
      'target_type' = 'turn',
      'method' = 'simple_mean',
      'mean' = a['Mean'],
      'median' = a['Median'],
      'var' = a['Variance']
    ))
  
  source('/home/rstudio/new_items_repredict.R')
  a <- describe(new_item_inv_data$inv_rel_diff1)
  master_df %<>% 
    rbind(., data.frame(
      'project_id' = project_id,
      'mod_drop_wk' = mod_drop_wk,
      'target_type' = 'turn',
      'method' = 're_sim_reg',
      'mean' = a['Mean'],
      'median' = a['Median'],
      'var' = a['Variance']
    ))
  a <- describe(new_item_inv_data$inv_rel_diff2)
  master_df %<>% 
    rbind(., data.frame(
      'project_id' = project_id,
      'mod_drop_wk' = mod_drop_wk,
      'target_type' = 'turn',
      'method' = 're_sim_mean',
      'mean' = a['Mean'],
      'median' = a['Median'],
      'var' = a['Variance']
    ))
  
  source('/home/rstudio/reconsider_new.R')
  a <- describe(new_item_inv_data$inv_rel_diff1)
  master_df %<>% 
    rbind(., data.frame(
      'project_id' = project_id,
      'mod_drop_wk' = mod_drop_wk,
      'target_type' = 'turn',
      'method' = 're_class_reg',
      'mean' = a['Mean'],
      'median' = a['Median'],
      'var' = a['Variance']
    ))
  a <- describe(new_item_inv_data$inv_rel_diff2)
  master_df %<>% 
    rbind(., data.frame(
      'project_id' = project_id,
      'mod_drop_wk' = mod_drop_wk,
      'target_type' = 'turn',
      'method' = 're_class_mean',
      'mean' = a['Mean'],
      'median' = a['Median'],
      'var' = a['Variance']
    ))
  source('/home/rstudio/project_details.R')
  return(master_df)
}


master_df_6190 <- run(6190, 201601)
master_df_4886 <- run(4886, 201634)
master_df_4458 <- run(4458, 201624)
master_df_5637 <- run(5637, 201720)
master_df_5723 <- run(5723, 201717)
master_df_6773 <- run(6773, 201708)
master_df_5328 <- run(5328, 201651)

# if (sim_method == 'one_by_one') {
#   source('sim_one_by_one.R')
# } else {
#   source('sim_sample.R')
# }
# 
# describe(sim$.)
# sim %>% ggplot(., aes(x = .)) + geom_density() + coord_cartesian(xlim = c(0,1))
# 
# 
# dataset.save(df = master_df, name = paste0('master_df_', getArgument('project_id')), over_write = TRUE)



master_df1 <- master_df %>% group_by(project_id, method, target_type) %>% summarise(m = mean(c(mean, median)))
master_df1 <- inner_join(master_df1, master_df1 %>% group_by(project_id) %>% summarise(m = min(m))) %>% 
  ungroup() %>% 
  mutate(aggregation = if_else(method %like% 'reg', 'linear_regression', 'weighted_mean'),
         method = if_else(method %like% 're_sim', '2nd_stage_similarity', 
                          if_else(method %like% 're_class', '2nd_stage_classification', 'simple'))
  )

all_proj <- ls()[ls() %like% 'project_details']

df_code <- paste('rbind(', paste(sapply(all_proj, function(x) paste0('get("', x, '")')), collapse = ', '), ')')
df_code <- eval(parse(text = df_code)) %>% inner_join(., master_df1)

pred_method <- rpart(method ~ ., data = df_code %>% select(-c(project_id, m, aggregation, target_type)), 
                     control = rpart.control(minsplit = 1, cp = 0))
pred_agg <- rpart(aggregation ~ ., data = df_code %>% select(-c(project_id, m, method, target_type)), 
                  control = rpart.control(minsplit = 1, cp = 0))
pred_target <- rpart(target_type ~ ., data = df_code %>% select(-c(project_id, m, aggregation, method)), 
                     control = rpart.control(minsplit = 1, cp = 0))

pred_method %>% predict(., get(paste0('project_details_', project_id)))
pred_agg %>% predict(., get(paste0('project_details_', project_id)))
pred_target %>% predict(., get(paste0('project_details_', project_id)))




