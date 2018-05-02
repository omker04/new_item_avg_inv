sim <- sapply(1:nrow(inv_data %>% filter(data_type == 'test')), function(y) {
  # sim <- sapply(1:500, function(y) {
  print(y)
  # s <- sample(x = unique((inv_data %>% filter(data_type == 'test'))$mds_fam_id), size = n)
  s <- inv_data %>% filter(data_type == 'test') %>% head(y) %>% tail(1)
  new_items <- inv_data %>% filter(mds_fam_id %in% s) %>% select(mds_fam_id) %>% unique()
  check_VI <- bbq_item_list %>%
    filter(!mds_fam_id %in% new_items$mds_fam_id & data_type == 'train') %>%
    select(-c(mds_fam_id, data_type)) %>%
    select(c(turn, everything())) %>% 
    unique() 
  all_data <- bbq_item_list %>%
    filter(!mds_fam_id %in% new_items$mds_fam_id & data_type == 'train') %>%
    select(-turn)
  train_data <- all_data %>%
    select(-c(mds_fam_id, data_type))
  test_data <- bbq_item_list %>%
    filter(mds_fam_id %in% new_items$mds_fam_id & data_type == 'test') %>%
    #select(brand_name, fineline_desc, avg_price)
    select(-c(mds_fam_id, turn, data_type))
  
  VI <- rpart(paste('turn ~ ', paste(colnames(check_VI)[-1], collapse = '+')), data = check_VI, 
              control = c(cp = 0, maxcompete = 10, maxsurrogate = 100, minsplit = 20)
  )$variable.importance
  maxcompete <- 10
  maxsurrogate <- 100
  minsplit <- 20
  while (is.null(VI)) {
    maxcompete <- maxcompete * 10 
    maxsurrogate <- maxcompete * 10
    minsplit <- max(1, floor(minsplit / 4))
    VI <- rpart(turn ~  ., data = check_VI, control = c(cp = 0, maxcompete = maxcompete, 
                                                        maxsurrogate = maxsurrogate, minsplit = minsplit)
    )$variable.importance
    if (is.null(VI) & minsplit == 1) {
      VI <- array(1, ncol(train_data)) %>% 
        set_names(colnames(train_data))
    }
  }
  labels <- all_data %>%
    select(mds_fam_id)
  
  weight_df <- data.frame(
    'colname' = colnames(train_data)
  ) %>% 
    left_join(., data.frame(VI) %>% mutate('colname' = rownames(.))) %>% 
    set_colnames(c('colname', 'imp')) %>% 
    mutate(imp = na.replace(imp, min(VI)))
  if(sum(weight_df$imp) < 1) {
    weight_df$imp <- weight_df$imp / sum(weight_df$imp)
  }
  dist_mtx <- Distance_for_KNN_test(train_set = train_data, test_set = test_data, VI = weight_df$imp)
  
  pred <- knn_test_function2(train_data, test_data, dist_mtx, labels$mds_fam_id, k) %>%
    set_names(., (bbq_item_list %>% filter(mds_fam_id %in% new_items$mds_fam_id & data_type == 'test')
    )$mds_fam_id)
  
  new_item_turn <- lapply(pred, function(x) {
    similar <- inv_data %>% 
      filter(mds_fam_id %in% as.numeric(x$neighbors) & data_type == 'train') %>% 
      mutate(turn = if_else(is.infinite(turn) | turn < 0, NA * 1, turn))
    weights <- data.frame(mds_fam_id = x$neighbors, weight = x$weights) %>% 
      inner_join(., similar) %>% 
      mutate(adj_weight = (weight * (wk_cnt / 52)) 
             #/ sum(weight * (wk_cnt / 52))
      )
    
    # turn <- sum(weights$weighted_turn, na.rm = TRUE) / sum(weights$weight * weights$wk_cnt / 52, na.rm = TRUE)
    turn <- mean_inlier(weights$turn, weights$adj_weight)
    
    return(turn)
  }) %>% do.call('rbind', .) %>%
    as.data.frame() %>%
    set_colnames('pred_turn') %>%
    mutate('mds_fam_id' = as.numeric(rownames(.)))
  
  if (inv == 'avg') {
    new_item_inv_data <- inv_data %>%
      filter(data_type == 'test') %>% 
      right_join(new_item_turn) %>%
      mutate('predicted_avg_inv' = store_count * wk_cnt * avg_price * upspw / pred_turn) %>%
      mutate('turn_rel_diff' = abs(turn - pred_turn) / (turn + pred_turn)) %>%
      mutate('inv_rel_diff' = abs(str_inv - predicted_avg_inv) / (str_inv + predicted_avg_inv))
  } else {
    new_item_inv_data <- inv_data %>%
      filter(data_type == 'test') %>% 
      right_join(new_item_turn) %>%
      mutate('predicted_avg_inv' = wk_cnt * avg_price * upspw / pred_turn) %>%
      mutate('turn_rel_diff' = abs(turn - pred_turn) / (turn + pred_turn)) %>%
      mutate('inv_rel_diff' = abs(str_inv - predicted_avg_inv) / (str_inv + predicted_avg_inv))
  }
  
  return(min(median(new_item_inv_data$turn_rel_diff, na.rm = TRUE), 
             mean(new_item_inv_data$turn_rel_diff, na.rm = TRUE)))
}) %>% data.frame()
