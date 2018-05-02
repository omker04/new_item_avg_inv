if (nrow(new_items) > 500 & uniqueN(inv_data$mds_fam_id) > 7000) {
  N <<- 20
}
sim <- lapply(1:N, function(y) {
  print(y)
  set.seed(24782 + y * 434)
  
  s <- sample(x = unique((inv_data %>% filter(data_type == 'test'))$mds_fam_id), size = nrow(new_items))
  if (nrow(new_items) > 500 & uniqueN(inv_data$mds_fam_id) > 7000) {
    s <- sample(x = unique((inv_data %>% filter(data_type == 'test'))$mds_fam_id), 
                size = 500)
  }
  
  new_items <- inv_data %>% filter(mds_fam_id %in% s) %>% select(mds_fam_id) %>% unique()
  check_VI <- bbq_item_list %>%
    filter(!mds_fam_id %in% new_items$mds_fam_id & data_type == 'train') %>%
    select(-c(mds_fam_id, data_type, upspw, store_count, tot_wkly_qty, str_inv, wk_cnt, ipspw)) %>%
    select(c(turn, everything())) %>% 
    unique() 
  
  all_data <- bbq_item_list %>%
    filter(!mds_fam_id %in% new_items$mds_fam_id & data_type == 'train') %>%
    select(-c(turn, data_type, upspw, store_count, tot_wkly_qty, str_inv, wk_cnt, ipspw))
  train_data <- all_data %>%
    select(-c(mds_fam_id))
  test_data <- bbq_item_list %>%
    filter(mds_fam_id %in% new_items$mds_fam_id & data_type == 'test') %>%
    select(-c(turn, data_type, upspw, store_count, tot_wkly_qty, str_inv, wk_cnt, ipspw)) %>% 
    unique()
  names <- test_data$mds_fam_id
  test_data %<>% 
    select(-mds_fam_id)
  
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
  weight_df$imp <- weight_df$imp / sum(weight_df$imp)
  
  if (nrow(test_data) > 700 | nrow(train_data) > 5000) {
    pred <- lapply(1:ceiling(nrow(test_data) / 500), function(x) {
      print(x)
      print(Sys.time())
      dist_mtx_ <- Distance_for_KNN_test(train_set = train_data, 
                                         test_set = test_data[((x-1) * 500 + 1) : min(nrow(test_data), x * 500),], 
                                         VI = weight_df$imp)
      pred_ <- knn_test_function2(train_data, 
                                  test_data[((x-1) * 500 + 1) : min(nrow(test_data), x * 500),], 
                                  dist_mtx_, labels$mds_fam_id, k/4) %>%
        set_names(., names[((x-1) * 500 + 1) : min(nrow(test_data), x * 500)])
      print(Sys.time())
      return(pred_)
    }) %>% do.call('c', .)
  } else {
    dist_mtx <- Distance_for_KNN_test(train_set = train_data, test_set = test_data, VI = weight_df$imp)
    
    pred <- knn_test_function2(train_data, test_data, dist_mtx, labels$mds_fam_id, k) %>%
      set_names(., names)
  }
  
  new_item_turn <- lapply(pred, function(x) {
    similar <- inv_data %>% 
      filter(mds_fam_id %in% as.numeric(x$neighbors) & data_type == 'train') %>% 
      mutate(turn = if_else(is.infinite(turn) | turn < 0, NA * 1, turn))
    weights <- data.frame(mds_fam_id = x$neighbors, weight = x$weights) %>% 
      inner_join(., similar, by = 'mds_fam_id') %>% 
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
  
  if (inv == 'rec') {
    new_item_inv_data <- inv_data %>%
      filter(data_type == 'test') %>% 
      right_join(new_item_turn) %>%
      mutate('predicted_avg_inv' = store_count * wk_cnt * avg_price * upspw * pred_turn) %>%
      mutate('turn_rel_diff' = abs(turn - pred_turn) / (turn + pred_turn)) %>%
      mutate('inv_rel_diff' = abs(str_inv - predicted_avg_inv) / (str_inv + predicted_avg_inv))
  } else {
    new_item_inv_data <- inv_data %>%
      filter(data_type == 'test') %>% 
      right_join(new_item_turn) %>%
      mutate('predicted_avg_inv' = store_count * wk_cnt * avg_price * upspw / pred_turn) %>%
      mutate('turn_rel_diff' = abs(turn - pred_turn) / (turn + pred_turn)) %>%
      mutate('inv_rel_diff' = abs(str_inv - predicted_avg_inv) / (str_inv + predicted_avg_inv))
  }
  
  a <- list('pred' = pred, 
            'new_item_inv_data' = new_item_inv_data)
  return(a)
})

new_item_inv_data <- lapply(sim, function(x) x$new_item_inv_data) %>% do.call('rbind', .)
pred <- lapply(sim, function(x) x$pred) %>% do.call('c', .)

new_item_inv_data %<>% mutate(confidence = cut(inv_rel_diff,breaks = c(0, 0.5, 1), labels = c('No Low', 'Low')))
no_low_pred <- pred[(new_item_inv_data %>% filter(confidence == 'No Low'))$mds_fam_id %>% as.character()]
low_pred <- pred[(new_item_inv_data %>% filter(confidence == 'Low'))$mds_fam_id %>% as.character()]

no_low_check <- lapply(no_low_pred, function(x) {
  similar <- inv_data %>% 
    filter(mds_fam_id %in% as.numeric(x$neighbors)) %>% 
    mutate(turn = if_else(is.infinite(turn) | turn < 0, NA * 1, turn))
  weights <- data.frame(mds_fam_id = x$neighbors, weight = x$weights) %>% 
    inner_join(., similar, by = 'mds_fam_id') %>% 
    mutate(adj_weight = if_else(data_type == 'train', weight * (wk_cnt / 52), weight * (wk_cnt / 27)) 
    )
  return(weights)
}) %>% do.call('rbind', .)

low_check <- lapply(low_pred, function(x) {
  similar <- inv_data %>% 
    filter(mds_fam_id %in% as.numeric(x$neighbors)) %>% 
    mutate(turn = if_else(is.infinite(turn) | turn < 0, NA * 1, turn))
  weights <- data.frame(mds_fam_id = x$neighbors, weight = x$weights) %>% 
    inner_join(., similar, by = 'mds_fam_id') %>% 
    mutate(adj_weight = if_else(data_type == 'train', weight * (wk_cnt / 52), weight * (wk_cnt / 27)) 
    )
  return(weights)
}) %>% do.call('rbind', .)

conf_checks <- low_check %>% 
  group_by(mds_fam_id) %>% 
  summarise(u_ratio = upspw[1] / upspw[2],
            u_sub = upspw[1] - upspw[2],
            s_ratio = store_count[1] / store_count[2],
            s_sub = store_count[1] - store_count[2],
            p_ratio = avg_price[1] / avg_price[2],
            p_sub = avg_price[1] - avg_price[2],
            w_mean = mean(adj_weight, na.rm = TRUE)) %>% 
  #select(-mds_fam_id) %>% 
  #apply(., 2, function(x) mean(x, na.rm = TRUE))
  mutate(conf = 'Low') %>% 
  rbind(., 
        no_low_check %>% 
          group_by(mds_fam_id) %>% 
          summarise(u_ratio = upspw[1] / upspw[2],
                    u_sub = upspw[1] - upspw[2],
                    s_ratio = store_count[1] / store_count[2],
                    s_sub = store_count[1] - store_count[2],
                    p_ratio = avg_price[1] / avg_price[2],
                    p_sub = avg_price[1] - avg_price[2],
                    w_mean = mean(adj_weight, na.rm = TRUE)) %>% 
          #select(-mds_fam_id) %>% 
          mutate(conf = 'No Low')
  ) 

new_tree <- rpart(conf ~ u_ratio + u_sub + s_ratio + s_sub + p_ratio + p_sub, data = conf_checks,
                  control = rpart.control(cp = 0, minsplit = 1, xval = 2))
conf_predict <- predict(new_tree, conf_checks %>% select(-mds_fam_id, -conf)) %>% as.data.frame()
no_low_cutoff <- quantile(conf_predict$Low, table(conf_checks$conf)['No Low'] / sum(table(conf_checks$conf)))


rm(list = c('low_pred', 'no_low_pred', 'low_check', 'no_low_check', 'pred'))

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

weight_df$imp <- weight_df$imp / sum(weight_df$imp)

dist_mtx <- Distance_for_KNN_test(train_set = train_data, test_set = test_data, VI = weight_df$imp)

pred <- knn_test_function2(train_data, test_data, dist_mtx, labels$mds_fam_id, k) %>%
  set_names(., (bbq_item_list %>% filter(mds_fam_id %in% new_items$mds_fam_id & data_type == 'test')
  )$mds_fam_id)

new_item_turn <- lapply(as.list(1:length(pred)), function(y) {
  x <- pred[[y]]
  x1 <- names(pred)[y]
  
  similar <- inv_data %>% 
    filter(mds_fam_id %in% as.numeric(x$neighbors)) %>% 
    inner_join(., 
               data.frame(mds_fam_id = x$neighbors, weight = x$weights) %>% 
                 group_by(mds_fam_id) %>% 
                 summarise(weight = mean(weight)), 
               by = 'mds_fam_id'
    ) %>% 
    mutate(adj_weight = if_else(data_type == 'train', weight * (wk_cnt / 52), weight * (wk_cnt / 26))) %>% 
    group_by(mds_fam_id) %>% 
    summarise(u_ratio = upspw[1] / upspw[2],
              u_sub = upspw[1] - upspw[2],
              s_ratio = store_count[1] / store_count[2],
              s_sub = store_count[1] - store_count[2],
              p_ratio = avg_price[1] / avg_price[2],
              p_sub = avg_price[1] - avg_price[2],
              w_mean = mean(adj_weight, na.rm = TRUE)
    )
  weight_df <- inv_data %>% 
    filter(mds_fam_id %in% as.numeric(x$neighbors) & data_type == 'train') %>% 
    mutate(turn = if_else(is.infinite(turn) | turn < 0, NA * 1, turn)) %>%  
    inner_join(., data.frame(mds_fam_id = x$neighbors, weight = x$weights) %>% 
                 group_by(mds_fam_id) %>% summarise(weight = mean(weight)), 
               by = 'mds_fam_id'
    ) %>% 
    mutate(adj_weight = weight * (wk_cnt / 52)) %>% 
    cbind(., predict(new_tree, similar %>% select(-mds_fam_id)) %>% as.data.frame()) %>% 
    mutate(conf_pred = if_else(Low < no_low_cutoff, 'No Low', if_else(Low > no_low_cutoff, 'Low', 'Doubt'))) 
  
  for (i in 1:nrow(weight_df)) {
    if (weight_df$conf_pred[i] == 'Doubt') {
      q <- weight_df$tot_wkly_qty[i] / max(weight_df$tot_wkly_qty + 1)
      s <- weight_df$store_count[i] / max(weight_df$store_count + 1)
      r <- rbinom(1, 1, (q + s) / 2)
      if (r < 0.5) {
        weight_df$conf_pred[i] <- 'Low'
      } else {
        weight_df$conf_pred[i] <- 'No Low'
      }
    }
  }
  
  if (is.na(table(weight_df$conf_pred)['No Low'])) {
    l <- lm(turn ~ store_count + upspw + avg_price, data = weight_df)
    t <- predict(l, inv_data %>% 
                   filter(mds_fam_id == as.numeric(x1)) %>% select(store_count, upspw, avg_price))
    t <- ifelse(t <= 0, mean_inlier(weight_df$turn, weight_df$adj_weight), t)
    k <- mean_inlier(weight_df$turn, weight_df$adj_weight)
  } else {
    weight_df %<>% 
      filter(conf_pred == 'No Low')
    l <- lm(turn ~ store_count + upspw + avg_price, data = weight_df)
    t <- predict(l, inv_data %>% 
                   filter(mds_fam_id == as.numeric(x1)
                          #& data_type == 'train'
                          ) %>% select(store_count, upspw, avg_price))
    t <- ifelse(t <= 0, mean_inlier(weight_df$turn, weight_df$adj_weight), t)
    k <- mean_inlier(weight_df$turn, weight_df$adj_weight)
  }
  
  #return(t)
  return(c(t, k))
  
}) %>% do.call('rbind', .) %>%
  as.data.frame() %>%
  set_colnames(c('pred_turn1', 'pred_turn2')) %>%
  mutate('mds_fam_id' = as.numeric(names(pred)))

if (inv == 'rec') {
  new_item_inv_data <- inv_data %>%
    filter(data_type == 'test') %>% 
    right_join(new_item_turn) %>%
    mutate('predicted_avg_inv1' = store_count * wk_cnt * avg_price * upspw * pred_turn1) %>%
    mutate('predicted_avg_inv2' = store_count * wk_cnt * avg_price * upspw * pred_turn2) %>%
    #mutate('turn_rel_diff' = abs(turn - pred_turn) / (turn + pred_turn)) %>%
    mutate('inv_rel_diff1' = abs(str_inv - predicted_avg_inv1) / (str_inv + predicted_avg_inv1)) %>% 
    mutate('inv_rel_diff2' = abs(str_inv - predicted_avg_inv2) / (str_inv + predicted_avg_inv2))
} else {
  new_item_inv_data <- inv_data %>%
    filter(data_type == 'test') %>% 
    right_join(new_item_turn) %>%
    mutate('predicted_avg_inv1' = store_count * wk_cnt * avg_price * upspw / pred_turn1) %>%
    mutate('predicted_avg_inv2' = store_count * wk_cnt * avg_price * upspw / pred_turn2) %>%
    #mutate('turn_rel_diff' = abs(turn - pred_turn) / (turn + pred_turn)) %>%
    mutate('inv_rel_diff1' = abs(str_inv - predicted_avg_inv1) / (str_inv + predicted_avg_inv1)) %>% 
    mutate('inv_rel_diff2' = abs(str_inv - predicted_avg_inv2) / (str_inv + predicted_avg_inv2))
}


#describe(new_item_inv_data$inv_rel_diff)
