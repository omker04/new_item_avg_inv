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
labels <- all_data %>%
  select(mds_fam_id)
rm(all_data)
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
rm(check_VI)
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

new_item_turn <- lapply(as.list(1 : length(pred)), function(y) {
  x <- pred[[y]]
  x1 <- names(pred)[y]
  similar <- inv_data %>% 
    filter(mds_fam_id %in% as.numeric(x$neighbors) & data_type =='train') %>% 
    mutate(turn = if_else(is.infinite(turn) | turn < 0, NA * 1, turn))
  weights <- data.frame(mds_fam_id = x$neighbors, weight = x$weights) %>% 
    inner_join(., similar, by = 'mds_fam_id') %>% 
    mutate(adj_weight = (weight * (wk_cnt / 52)) 
           #/ sum(weight * (wk_cnt / 52))
    )
  
  k <- mean_inlier(weights$turn, weights$adj_weight)
  l <- lm(turn ~ store_count + upspw + avg_price, data = weights)        
  t <- predict(l, inv_data %>% 
                 filter(mds_fam_id == as.numeric(x1)) %>% 
                 select(store_count, upspw, avg_price)
               )
  t <- ifelse(t <= 0, k, t)
  
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
# 
# new_item_inv_data <- inv_data %>%
#   filter(data_type == 'test') %>% 
#   right_join(new_item_turn) %>%
#   mutate('predicted_avg_inv' = store_count * wk_cnt * avg_price * upspw * pred_turn) %>%
#   mutate('turn_rel_diff' = abs(turn - pred_turn) / (turn + pred_turn)) %>%
#   mutate('inv_rel_diff' = abs(str_inv - predicted_avg_inv) / (str_inv + predicted_avg_inv))
# 
# if (inv == 'rec') {
#   new_item_inv_data <- inv_data %>%
#     filter(data_type == 'test') %>% 
#     right_join(new_item_turn) %>%
#     mutate('predicted_avg_inv' = store_count * wk_cnt * avg_price * upspw * pred_turn) %>%
#     mutate('turn_rel_diff' = abs(turn - pred_turn) / (turn + pred_turn)) %>%
#     mutate('inv_rel_diff' = abs(str_inv - predicted_avg_inv) / (str_inv + predicted_avg_inv))
# } else {
#   new_item_inv_data <- inv_data %>%
#     filter(data_type == 'test') %>% 
#     right_join(new_item_turn) %>%
#     mutate('predicted_avg_inv' = store_count * wk_cnt * avg_price * upspw / pred_turn) %>%
#     mutate('turn_rel_diff' = abs(turn - pred_turn) / (turn + pred_turn)) %>%
#     mutate('inv_rel_diff' = abs(str_inv - predicted_avg_inv) / (str_inv + predicted_avg_inv))
# }
# 
# describe(new_item_inv_data$inv_rel_diff)
# 
# print(new_item_inv_data %>% ggplot(., aes(x = inv_rel_diff)) + geom_density() + coord_cartesian(xlim = c(0,1)))
# 
# inv_data_validate <- inv_data %>%
#   filter(data_type == 'test'
#          & mds_fam_id %in% new_items$mds_fam_id
#   ) %>%
#   cbind(.,
#         new_item_inv_data %>% select(inv_rel_diff) %>% set_colnames('mape')
#   )
# 
# 
# group_by_qtle <- function(x) {
#   if (is.numeric(x)) {
#     # q <- quantile(x, probs = c(0, 0.2, 0.4, 0.6, 0.8, 1), na.rm = TRUE)
#     # y <- cut(x, breaks = q, include.lowest = FALSE, labels = c('VL', 'L', 'M', 'H', 'VH'))
#     # y[is.na(y)] <- 'VL'
#     q <- quantile(x, probs = c(0, 0.33, 0.67, 1), na.rm = TRUE)
#     y <- cut(x, breaks = q, include.lowest = FALSE, labels = c('L', 'M', 'H'))
#     y[is.na(y)] <- 'L'
#     return(y)
#   } else {
#     if (is.na(as.numeric(x))) {
#       return(x)
#     } else {
#       x <- as.numeric(x)
#       # q <- quantile(x, probs = c(0, 0.2, 0.4, 0.6, 0.8, 1), na.rm = TRUE)
#       # y <- cut(x, breaks = q, include.lowest = FALSE, labels = c('VL', 'L', 'M', 'H', 'VH'))
#       # y[is.na(y)] <- 'VL'
#       q <- quantile(x, probs = c(0, 0.33, 0.67, 1), na.rm = TRUE)
#       y <- cut(x, breaks = q, include.lowest = FALSE, labels = c('L', 'M', 'H'))
#       y[is.na(y)] <- 'L'
#       return(y)
#     }
#   }
# }
# 
# inv_data_treatment <- inv_data %>%
#   filter(data_type == 'test'
#          & mds_fam_id %in% new_items$mds_fam_id
#   ) %>%
#   # filter(tot_wkly_qty > 0) %>%
#   # inner_join(., bbq_item_list %>% select(-turn, -avg_price), by = 'mds_fam_id') %>%
#   # left_join(., bbq_item_list %>% select(mds_fam_id, item_type_desc) %>% unique()) %>%
#   select(tot_wkly_qty, avg_price, upspw, str_inv, store_count) %>%
#   # mutate('ipspw' = str_inv / store_count) %>%
#   select(upspw, store_count, avg_price) %>%
#   mutate_all(funs(as.character)) %>%
#   mutate_all(funs(group_by_qtle))
# 
# 
# ctree(mape ~ .,
#       data = inv_data_validate %>% #filter(tot_wkly_qty > 0) %>%
#         select(mape) %>% cbind(., inv_data_treatment) %>% mutate(mape = na.replace(mape, 0)),
#       mincriterion = 0.4) %>% plot()
# 
# 
