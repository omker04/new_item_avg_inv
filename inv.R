library(dtplyr)
library(dplyr)
library(data.table)
library(magrittr)
library(cluster)
library(FastKNN)
library(mlutils)

get_items <- dataset.load(name = 'myHiveConnector', query = 'select * from omahala.all_item_upspw_bbq;')
inv_proxy <- dataset.load(name = 'myHiveConnector', query = 'select * from omahala.inv_proxy_bbq;')

whse_inv_query <- paste(
  'select item_nbr, SUM(whpk_sell_price * on_hand_qty) / count(distinct wm_yr_wk) as whse_inv from wm_user.whse_wkly_sku',
  'where item_nbr in (', paste0(get_items$mds_fam_id, collapse = ', '), ') and wm_yr_wk between 11508 and 11608 group by item_nbr;'
)
whse_inv <- dataset.load(name = 'smishr2 teradata connector', query = whse_inv_query)

str_inv_query <- paste(
  'select item_nbr, avg(on_hand_rtl_amt) as str_inv from wm_user.item_wkly_inv',
  'where item_nbr in (', paste0(get_items$mds_fam_id, collapse = ', '), 
") and wm_yr_wk between 11508 and 11608 and on_hand_rtl_amt >= 0 and country_code = 'US' and company_code = 'wm' group by item_nbr;"
)
str_inv <- dataset.load(name = 'smishr2 teradata connector', query = str_inv_query)

na.replace <- function(x, t = 0) {
  x[is.na(x)] <- t
  return(x)
}

inv_data <- get_items %>% 
  left_join(., str_inv, by = c('mds_fam_id' = 'ITEM_NBR')) %>% 
  left_join(., whse_inv, by = c('mds_fam_id' = 'ITEM_NBR')) %>% 
  left_join(inv_proxy) %>% 
  mutate('avg_inv' = na.replace(str_inv) + na.replace(whse_inv)) %>% 
  mutate('turn' = avg_price * upspw / avg_inv,
         'proxy_turn' = upspw / inv_proxy) %>% 
  mutate(weighted_turn = if_else(is.infinite(turn) | is.na(turn), proxy_turn, (turn + proxy_turn) / 2))


item_desc <- dataset.load(name = 'myHiveConnector', 
                          query = 'select distinct mds_fam_id, brand_name, fineline_desc, whpk_qty, 
                          item_length_qty, item_height_qty, item_width_qty, item_weight_qty from omahala.new_item_identification_try_bbq;')

bbq_item_list <- item_desc %>% 
  group_by(mds_fam_id) %>% 
  left_join(., inv_data %>% select(mds_fam_id, avg_price, turn, proxy_turn, weighted_turn)) %>% 
  unique() %>% 
  ungroup() %>% 
  mutate(fineline_desc = as.numeric(as.factor(fineline_desc)),
         brand_name = as.numeric(as.factor(brand_name)),
         item_length_qty = as.numeric(item_length_qty),
         item_height_qty = as.numeric(item_height_qty),
         item_width_qty = as.numeric(item_width_qty),
         item_weight_qty = as.numeric(item_weight_qty)
  )

knn_test_function2 <- function(dataset, test, distance, labels, k = 3){
  if (ncol(dataset) != ncol(test)) 
    stop("The test set and the training set have a different number of features")
  predictions <- vector('list', nrow(test))
  for (i in 1:nrow(test)) {
    indices <- FastKNN::k.nearest.neighbors(i, distance, k = k)
    vec_mas_cer <- table(labels[indices])
    predictions[[i]] <- as.numeric(sample(names(which(vec_mas_cer == max(vec_mas_cer))), size = k, replace = TRUE))
  }
  return(predictions)
}



cutoff <- quantile(inv_data$tot_wkly_qty[inv_data$tot_wkly_qty > 0], 0.1)
inv_data %<>% 
  filter(tot_wkly_qty >= cutoff)
bbq_item_list %<>% 
  filter(mds_fam_id %in% inv_data$mds_fam_id)



sim <- sapply(1:500, function(y) {
  print(y)
  s <- sample(x = inv_data$mds_fam_id, size = 150)
  new_items <- inv_data %>% filter(mds_fam_id %in% s)
  #new_items <- dataset.load(name = 'myHiveConnector', query = 'select * from omahala.new_item_bbq where c = 1;')
  train_data <- bbq_item_list %>% 
    filter(!mds_fam_id %in% new_items$mds_fam_id) %>% 
    select(-c(mds_fam_id, turn, proxy_turn, weighted_turn))
  test_data <- bbq_item_list %>% 
    filter(mds_fam_id %in% new_items$mds_fam_id) %>% 
    select(-c(mds_fam_id, turn, proxy_turn, weighted_turn))
  labels <- bbq_item_list %>% 
    filter(!mds_fam_id %in% new_items$mds_fam_id) %>% 
    select(mds_fam_id)
  dist_mtx <- Distance_for_KNN_test(train_set = train_data, test_set = test_data)
  
  k = 1
  pred <- knn_test_function2(train_data, test_data, dist_mtx, labels$mds_fam_id, k) %>% 
    set_names(., (bbq_item_list 
                  %>% filter(mds_fam_id %in% new_items$mds_fam_id)
    )$mds_fam_id)
  
  new_item_turn <- lapply(pred, function(x) {
    similar <- inv_data %>% filter(mds_fam_id %in% as.numeric(x))
    # similar$turn[is.infinite(similar$turn)] <- NA
    # similar$turn[is.nan(similar$turn)] <- NA
    turn <- mean(similar$weighted_turn, na.rm = TRUE)
    return(turn)
  }) %>% do.call('rbind', .) %>% 
    as.data.frame() %>% 
    set_colnames('pred_turn') %>% 
    mutate('mds_fam_id' = as.numeric(rownames(.)))
  
  new_item_inv_data <- inv_data %>% 
    right_join(new_item_turn) %>% 
    mutate('predicted_avg_inv' = avg_price * upspw / pred_turn) %>% 
    mutate('turn_rel_diff' = abs(weighted_turn - pred_turn) / (weighted_turn + pred_turn)) %>% 
    mutate('inv_rel_diff' = abs(inv_proxy - predicted_avg_inv) / (inv_proxy + predicted_avg_inv))
  
  return(median(new_item_inv_data$inv_rel_diff, na.rm = TRUE))
})

similarity <- 'brand_name'
bbq_item_list <- item_desc %>% 
  group_by(mds_fam_id) %>% 
  left_join(., inv_data %>% select(mds_fam_id, avg_price, turn, proxy_turn, weighted_turn)) %>% 
  unique() %>% 
  ungroup()

brand_sim <- sapply(1:500, function(y) {
  print(y)
  s <- sample(x = inv_data$mds_fam_id, size = 150)
  new_items <- inv_data %>% filter(mds_fam_id %in% s)
  new_item_turn <- lapply(new_items$mds_fam_id, function(x) {
    new_item_brand <- bbq_item_list %>% filter(mds_fam_id == x) %>% select_(.dots = lapply(similarity, function(x) as.symbol(x)))
    similar <- inv_data %>% 
      right_join(., bbq_item_list %>% right_join(., new_item_brand))
    turn <- mean(similar$weighted_turn, na.rm = TRUE)
    return(turn)
  }) %>% 
    set_names(new_items$mds_fam_id) %>% 
    do.call('rbind', .) %>% 
    as.data.frame() %>% 
    set_colnames('pred_turn') %>% 
    mutate('mds_fam_id' = as.numeric(rownames(.)))
  new_item_inv_data <- inv_data %>% 
    right_join(new_item_turn) %>% 
    mutate('predicted_avg_inv' = avg_price * upspw / pred_turn) %>% 
    mutate('turn_rel_diff' = abs(weighted_turn - pred_turn) / (weighted_turn + pred_turn)) %>% 
    mutate('inv_rel_diff' = abs(inv_proxy - predicted_avg_inv) / (inv_proxy + predicted_avg_inv))
  
  return(median(new_item_inv_data$inv_rel_diff, na.rm = TRUE))
})

sapply(pred, function(x) {
  y <- inv_data %>% filter(mds_fam_id %in% as.numeric(x))
  p <- ks.test(y$weighted_turn - mean(y$weighted_turn, na.rm = TRUE), runif(100, -0.01, 0.01))$p.value
  return(ifelse(p <= 0.05, 0, 1))
}) %>% sum()


