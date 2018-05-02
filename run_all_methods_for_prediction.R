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

rm(list = ls())
# project_id <<- as.numeric(proj)
# mod_drop_wk <<- as.numeric(mod_drop)
# source('/home/rstudio/new_functions.R')

project_id <<- getArgument('project_id') %>% as.numeric()
mod_drop_wk <<- getArgument('mod_drop_wk') %>% as.numeric()

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

  # master_df <- dataset.load(name = 'master_df') %>% unique()
  # if (!exists('master_df')) {
  #   master_df <- data.frame(
  #     'project_id' = 1,
  #     'mod_drop_wk' = 1,
  #     'target_type' = 1,
  #     'method' = 1,
  #     'mean' = 1,
  #     'median' = 1,
  #     'var' = 1
  #   ) %>% filter(project_id > 1)
  # }

  source('/home/rstudio/user_defined_functions.R')
  source('/home/rstudio/data_prep.R')

  source('/home/rstudio/new_items.R')
  a <- describe(new_item_inv_data$inv_rel_diff1)
  connector.execute(name = 'myHiveConnector_new_db', statement = paste0('insert into table new_item_avg_inv.master_df values (', 
                                                                        project_id, ', ', mod_drop_wk, ', ',
                                                                        '"inverse turn", ', '"simple_reg", ', 
                                                                        a['Mean'], ', ', a['Median'], ', ', na.replace(a['Variance'], 0), ', ', project_id, ');')
  )
  a <- describe(new_item_inv_data$inv_rel_diff2)
  connector.execute(name = 'myHiveConnector_new_db', statement = paste0('insert into table new_item_avg_inv.master_df values (', 
                                                                        project_id, ', ', mod_drop_wk, ', ',
                                                                '"inverse turn", ', '"simple_mean", ', 
                                                                a['Mean'], ', ', a['Median'], ', ', na.replace(a['Variance'], 0), ', ', project_id, ');')
  )
  new_item_inv_data_all <- new_item_inv_data %>% 
    mutate(method = 'simple', 
           target = 'inverse turn', 
           sl = 1)

  source('/home/rstudio/new_items_repredict.R')
  a <- describe(new_item_inv_data$inv_rel_diff1)
  connector.execute(name = 'myHiveConnector_new_db', statement = paste0('insert into table new_item_avg_inv.master_df values (', 
                                                                        project_id, ', ', mod_drop_wk, ', ',
                                                                '"inverse turn", ', '"re_sim_reg", ', 
                                                                a['Mean'], ', ', a['Median'], ', ', na.replace(a['Variance'], 0), ', ', project_id, ');')
  )
  a <- describe(new_item_inv_data$inv_rel_diff2)
  connector.execute(name = 'myHiveConnector_new_db', statement = paste0('insert into table new_item_avg_inv.master_df values (', 
                                                                        project_id, ', ', mod_drop_wk, ', ',
                                                                '"inverse turn", ', '"re_sim_mean", ', 
                                                                a['Mean'], ', ', a['Median'], ', ', na.replace(a['Variance'], 0), ', ', project_id, ');')
  )
  new_item_inv_data_all %<>% rbind(.,
                                   new_item_inv_data %>%
                                     mutate(method = '2nd_stage_similarity', 
                                            target = 'inverse turn',
                                            sl = 2)
  )

  source('/home/rstudio/reconsider_new.R')
  a <- describe(new_item_inv_data$inv_rel_diff1)
  connector.execute(name = 'myHiveConnector_new_db', statement = paste0('insert into table new_item_avg_inv.master_df values (', 
                                                                        project_id, ', ', mod_drop_wk, ', ',
                                                                '"inverse turn", ', '"re_class_reg", ', 
                                                                a['Mean'], ', ', a['Median'], ', ', na.replace(a['Variance'], 0), ', ', project_id, ');')
  )
  a <- describe(new_item_inv_data$inv_rel_diff2)
  connector.execute(name = 'myHiveConnector_new_db', statement = paste0('insert into table new_item_avg_inv.master_df values (', 
                                                                        project_id, ', ', mod_drop_wk, ', ',
                                                                '"inverse turn", ', '"re_class_mean", ', 
                                                                a['Mean'], ', ', a['Median'], ', ', na.replace(a['Variance'], 0), ', ', project_id, ');')
  )
  new_item_inv_data_all %<>% rbind(.,
                                   new_item_inv_data %>%
                                     mutate(method = '2nd_stage_classification', 
                                            target = 'inverse turn',
                                            sl = 3)
  )

  inv <<- 'rec1'

  source('/home/rstudio/user_defined_functions.R')
  source('/home/rstudio/data_prep.R')

  source('/home/rstudio/new_items.R')
  a <- describe(new_item_inv_data$inv_rel_diff1)
  connector.execute(name = 'myHiveConnector_new_db', statement = paste0('insert into table new_item_avg_inv.master_df values (', 
                                                                        project_id, ', ', mod_drop_wk, ', ',
                                                                '"turn", ', '"simple_reg", ', 
                                                                a['Mean'], ', ', a['Median'], ', ', na.replace(a['Variance'], 0), ', ', project_id, ');')
  )
  a <- describe(new_item_inv_data$inv_rel_diff2)
  connector.execute(name = 'myHiveConnector_new_db', statement = paste0('insert into table new_item_avg_inv.master_df values (', 
                                                                        project_id, ', ', mod_drop_wk, ', ',
                                                                '"turn", ', '"simple_mean", ', 
                                                                a['Mean'], ', ', a['Median'], ', ', na.replace(a['Variance'], 0), ', ', project_id, ');')
  )
  new_item_inv_data_all %<>% rbind(.,
                                   new_item_inv_data %>%
                                     mutate(method = 'simple', 
                                            target = 'turn',
                                            sl = 4)
  )

  source('/home/rstudio/new_items_repredict.R')
  a <- describe(new_item_inv_data$inv_rel_diff1)
  connector.execute(name = 'myHiveConnector_new_db', statement = paste0('insert into table new_item_avg_inv.master_df values (', 
                                                                        project_id, ', ', mod_drop_wk, ', ',
                                                                '"turn", ', '"re_sim_reg", ', 
                                                                a['Mean'], ', ', a['Median'], ', ', na.replace(a['Variance'], 0), ', ', project_id, ');')
  )
  a <- describe(new_item_inv_data$inv_rel_diff2)
  connector.execute(name = 'myHiveConnector_new_db', statement = paste0('insert into table new_item_avg_inv.master_df values (', 
                                                                        project_id, ', ', mod_drop_wk, ', ',
                                                                '"turn", ', '"re_sim_mean", ', 
                                                                a['Mean'], ', ', a['Median'], ', ', na.replace(a['Variance'], 0), ', ', project_id, ');')
  )
  new_item_inv_data_all %<>% rbind(.,
                                   new_item_inv_data %>%
                                     mutate(method = '2nd_stage_similarity', 
                                            target = 'turn',
                                            sl = 5)
  )

  source('/home/rstudio/reconsider_new.R')
  a <- describe(new_item_inv_data$inv_rel_diff1)
  connector.execute(name = 'myHiveConnector_new_db', statement = paste0('insert into table new_item_avg_inv.master_df values (', 
                                                                          project_id, ', ', mod_drop_wk, ', ',
                                                                '"turn", ', '"re_class_reg", ', 
                                                                a['Mean'], ', ', a['Median'], ', ', na.replace(a['Variance'], 0), ', ', project_id, ');')
  )
  a <- describe(new_item_inv_data$inv_rel_diff2)
  connector.execute(name = 'myHiveConnector_new_db', statement = paste0('insert into table new_item_avg_inv.master_df values (', 
                                                                        project_id, ', ', mod_drop_wk, ', ',
                                                                '"turn", ', '"re_class_mean", ', 
                                                                a['Mean'], ', ', a['Median'], ', ', na.replace(a['Variance'], 0), ', ', project_id, ');')
  )
  new_item_inv_data_all %<>% rbind(.,
                                   new_item_inv_data %>%
                                     mutate(method = '2nd_stage_classification', 
                                            target = 'turn',
                                            sl = 6)
  )
  source('/home/rstudio/project_details.R')
  new_item_inv_data_all
  return(new_item_inv_data_all)
}

result <- run(getArgument("project_id") %>% as.numeric(), 
              getArgument("mod_drop_wk") %>% as.numeric())
output <- result

output_edit <- rbind(
  output %>% 
    group_by(method, target) %>% 
    summarise(m = (mean(inv_rel_diff1, na.rm = TRUE) + median(inv_rel_diff1, na.rm = TRUE)) / 2,
              sl = mean(sl)) %>% 
    mutate(sl0 = 1),
  output %>% 
    group_by(method, target) %>% 
    summarise(m = (mean(inv_rel_diff2, na.rm = TRUE) + median(inv_rel_diff2, na.rm = TRUE)) / 2,
              sl = mean(sl)) %>% 
    mutate(sl0 = 2)
  ) %>% 
  arrange(m) %>% 
  ungroup() %>% 
  filter(m == min(m))

print(output_edit)

final_output <- output %>% 
  filter(sl == output_edit$sl[1]) %>% 
  select(mds_fam_id, tot_wkly_qty, avg_price, upspw, store_count, data_type, str_inv, ipspw, turn)

if (output_edit$sl0[1] == 1) {
  final_output %<>% 
    mutate(predicted_avg_inv = output$predicted_avg_inv1[output$sl == output_edit$sl[1]],
           predicted_error = output$inv_rel_diff1[output$sl == output_edit$sl[1]]
           )
} else {
  final_output %<>% 
    mutate(predicted_avg_inv = output$predicted_avg_inv2[output$sl == output_edit$sl[1]],
           predicted_error = output$inv_rel_diff2[output$sl == output_edit$sl[1]]
    )
}

write.csv(x = final_output, 
          file = paste0('/home/rstudio/original_best_output_', project_id, '.csv'), 
          row.names = FALSE)

# 
# pushVariable('p1', as.character(end_wk))
# 

