check_VI <- bbq_item_list %>%
  filter(!mds_fam_id %in% new_items$mds_fam_id & data_type == 'train') %>%
  select(-c(mds_fam_id, data_type, upspw, store_count, tot_wkly_qty, str_inv, wk_cnt, ipspw)) %>%
  select(c(turn, everything())) %>% 
  unique() 

all_data <- bbq_item_list %>%
  filter(!mds_fam_id %in% new_items$mds_fam_id & data_type == 'train') %>%
  select(-c(turn, data_type))#, upspw, store_count, tot_wkly_qty, str_inv, wk_cnt, ipspw))

train_data <- all_data %>%
  select(-c(mds_fam_id, upspw, store_count, tot_wkly_qty, str_inv, wk_cnt, ipspw))

all_test_data <- bbq_item_list %>%
  filter(mds_fam_id %in% new_items$mds_fam_id & data_type == 'test') %>%
  select(-c(turn, data_type))#, upspw, store_count, tot_wkly_qty, str_inv, wk_cnt, ipspw))
test_data <- all_test_data %>%
  select(-c(mds_fam_id, upspw, store_count, tot_wkly_qty, str_inv, wk_cnt, ipspw))

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
      set_names(., (bbq_item_list %>% filter(mds_fam_id %in% new_items$mds_fam_id[((x-1) * 500 + 1) : min(nrow(test_data), x * 500)] & 
                                               data_type == 'test'))$mds_fam_id)
    print(Sys.time())
    return(pred_)
  }) %>% do.call('c', .)
} else {
  dist_mtx <- Distance_for_KNN_test(train_set = train_data, test_set = test_data[1:1000,], VI = weight_df$imp)
  
  pred <- knn_test_function2(train_data, test_data, dist_mtx, labels$mds_fam_id, k) %>%
    set_names(., (bbq_item_list %>% filter(mds_fam_id %in% new_items$mds_fam_id & data_type == 'test')
    )$mds_fam_id)
}

new_item_turn <- lapply(as.list(1:length(pred)), function(y) {
  x <- pred[[y]]
  x1 <- names(pred)[y]
  
  train_2 <- all_data %>% 
    filter(mds_fam_id %in% x$neighbors) %>% 
    select(mds_fam_id, upspw, store_count, avg_price)
  test_2 <- all_test_data %>% 
    filter(mds_fam_id == as.numeric(x1)) %>% 
    select(upspw, store_count, avg_price)
  d <- Distance_for_KNN_test(test_set = test_2, train_set = train_2 %>% select(-mds_fam_id), VI = c(1, 1, 1))
  p <- knn_test_function2(dataset = train_2 %>% select(-mds_fam_id), test = test_2, distance = d, labels = train_2$mds_fam_id, k = 20)
  
  similar <- inv_data %>% 
    filter(mds_fam_id %in% as.numeric(p[[1]]$neighbors) & data_type == 'train') %>% 
    mutate(turn = if_else(is.infinite(turn) | turn < 0, NA * 1, turn)) %>% 
    inner_join(., data.frame(mds_fam_id = x$neighbors, weight = x$weights), by = 'mds_fam_id') %>% 
    mutate(adj_weight = (weight * (wk_cnt / 52)))
  
  k <- mean_inlier((similar %>% filter(data_type == 'train'))$turn, (similar %>% filter(data_type == 'train'))$adj_weight)
  l <- lm(turn ~ store_count + upspw + avg_price, data = similar)
  t <- predict(l, test_2)
  t <- ifelse(t <= 0, k, t)
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
