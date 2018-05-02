get_items <- dataset.load(name = 'myHiveConnector_new_db', query = paste0('select * from new_item_avg_inv.all_item_upspw_', project_id)) %>% 
  # read.csv(paste0('get_items_', project_id, '.csv'), sep = '\t') %>% 
  set_colnames(., gsub(pattern = paste0('all_item_upspw_', project_id, '.'), replacement = '', x = colnames(.))) %>% 
  mutate(avg_price = as.numeric(as.character(avg_price)),
         upspw = as.numeric(as.character(upspw)))

query <- paste0(
  'select item_nbr, sum(on_hand_rtl_amt) as str_inv, count(distinct wm_yr_wk) as wk_cnt, data_type from ( ',
  #"select item_nbr, on_hand_rtl_amt, case when wm_yr_wk < ", gsub(20, 1, end_wk - 100), " then 'inv_same' when wm_yr_wk between ", gsub(20, 1, end_wk - 100), " and ", gsub(20, 1, mod_drop_wk), " then 'other train' else 'test' end as data_type, wm_yr_wk ",
  "select item_nbr, on_hand_rtl_amt, case when wm_yr_wk < ", paste0(1, mod_drop_wk - 200000), " then 'train' else 'test' end as data_type, wm_yr_wk ",
  'from wm_user.item_wkly_inv ',
  'where wm_yr_wk between ', paste0(1, mod_drop_wk - 200000 - 100), ' and ',  paste0(1, end_wk - 200000), ' and ',
  'item_nbr in ( ',
  paste(unique(get_items$mds_fam_id), collapse = ', '),
  ' ) ) a ',
  'group by item_nbr, data_type ;'
)

str_inv <- dataset.load(name = 'smishr2 teradata connector', query = query)

inv_data <- get_items %>% 
  inner_join(., str_inv, by = c('mds_fam_id' = 'ITEM_NBR', 'data_type' = 'data_type')) %>% 
  # mutate_all(funs(as.character)) %>% 
  # mutate_all(funs(as.numeric), -data_type) %>% 
  mutate('ipspw' = str_inv / store_count,
         'turn' = (avg_price * upspw) / (str_inv / (wk_cnt * store_count))
  ) %>% 
  filter(str_inv > 0 & tot_wkly_qty > 0) 

table_data <-  dataset.load(name = 'myHiveConnector_new_db', query = paste0('select * from new_item_avg_inv.new_item_identification_try_', project_id)) %>% 
  # read.csv(paste0('item_desc_', project_id, '.csv'), sep = '\t') %>% 
  set_colnames(., gsub(pattern = paste0('new_item_identification_try_', project_id, '.'), replacement = '', x = colnames(.))) %>% 
  inner_join(., inv_data %>% select(mds_fam_id, avg_price, turn, data_type, upspw, store_count)) %>% 
  unique() %>% 
  ungroup() %>% 
  mutate(item_length_qty = as.numeric(item_length_qty),
         item_height_qty = as.numeric(item_height_qty),
         item_width_qty = as.numeric(item_width_qty),
         item_weight_qty = as.numeric(item_weight_qty)
  ) %>% 
  mutate(item_volume_qty = item_length_qty * item_height_qty * item_width_qty) %>% 
  select(-turns) %>% 
  right_join(., inv_data %>% select(mds_fam_id)) 

bbq_item_list <- table_data %>% 
  select(mds_fam_id, data_type, avg_price, brand_name, brand_family_name, fineline_desc, whpk_unit_cost, whpk_qty, 
         item_type_desc, item_volume_qty, item_weight_qty, turn, upspw, store_count) %>% 
  unique()

cutoff <- quantile(inv_data$tot_wkly_qty[inv_data$tot_wkly_qty > 0], 0.1)

inv_data %<>%
  mutate(data_type = as.character(data_type)) %>%
  filter(tot_wkly_qty >= cutoff) %>%
  filter(!is.na(str_inv)) %>%
  filter(str_inv > 0) 

bbq_item_list %<>%
  inner_join(., inv_data)

new_items <- bbq_item_list %>% 
  mutate(indicator = if_else(data_type == 'train', 1, -1)) %>% 
  group_by(mds_fam_id) %>% 
  summarise(n = sum(indicator)) %>% 
  ungroup() %>% 
  filter(n == -1 & mds_fam_id %in% (inv_data %>% filter(data_type == 'test'))$mds_fam_id) %>% 
  select(mds_fam_id)


nbr_items_in_proj <- uniqueN(get_items$mds_fam_id)
nbr_new_items <- uniqueN(new_items$mds_fam_id)

d1 <- bbq_item_list %>% select(mds_fam_id, brand_name) %>% unique() %>% select(brand_name)
nbr_items_per_brand <- table(d1$brand_name) %>% mean()
med_nbr_items_per_brand <- table(d1$brand_name) %>% median()
var_nbr_items_per_brand <- table(d1$brand_name) %>% var()

d1 <- bbq_item_list %>% select(mds_fam_id, fineline_desc) %>% unique() %>% select(fineline_desc)
nbr_items_per_fineline <- table(d1$fineline_desc) %>% mean()
med_nbr_items_per_fineline <- table(d1$fineline_desc) %>% median()
var_nbr_items_per_fineline <- table(d1$fineline_desc) %>% var()

items_per_dollar <- (bbq_item_list %>% mutate(items_per_dollar = 1 / avg_price) %>% select(items_per_dollar))$items_per_dollar %>% mean()
var_items_per_dollar <- (bbq_item_list %>% mutate(items_per_dollar = 1 / avg_price) %>% select(items_per_dollar))$items_per_dollar %>% var()

df1 <- get_items %>% group_by(mds_fam_id) %>% summarise(avg_store_count = mean(store_count), avg_qty = mean(tot_wkly_qty))
store_count_per_item <- mean(df1$avg_store_count)
var_store_count_per_item <- var(df1$avg_store_count)

wkly_qty <- mean(df1$avg_qty)
var_wkly_qty <- var(df1$avg_qty)

nbr_items_inv_avbl <- uniqueN(str_inv$ITEM_NBR)
df2 <- str_inv %>% group_by(ITEM_NBR) %>% summarise(inv = mean(str_inv))
inv_per_item <- mean(df2$inv)
var_inv_per_item <- var(df2$inv)

turn <- mean(inv_data$turn)
var_turn <- var(inv_data$turn)

assign(x = paste0('project_details_', project_id), data.frame(
  'project_id' = project_id,
  'nbr_items_in_proj' = nbr_items_in_proj,
  'nbr_new_items' = nbr_new_items,
  'nbr_items_per_brand' = nbr_items_per_brand,
  'med_nbr_items_per_brand' = med_nbr_items_per_brand,
  'var_nbr_items_per_brand' = var_nbr_items_per_brand,
  'nbr_items_per_fineline' = nbr_items_per_fineline,
  'med_nbr_items_per_fineline' = med_nbr_items_per_fineline,
  'var_nbr_items_per_fineline' = var_nbr_items_per_fineline,
  'items_per_dollar' = items_per_dollar,
  'var_items_per_dollar' = var_items_per_dollar,
  'store_count_per_item' = store_count_per_item,
  'var_store_count_per_item' = var_store_count_per_item,
  'wkly_qty' = wkly_qty,
  'med_wkly_qty' = median(df1$avg_qty),
  'var_wkly_qty' = var_wkly_qty,
  'nbr_items_inv_avbl' = nbr_items_inv_avbl,
  'inv_per_item' = inv_per_item,
  'med_inv_per_item' = median(df2$inv),
  'var_inv_per_item' = var_inv_per_item,
  'turn' = turn,
  'var_turn' = var_turn,
  'inv_to_presence' = nbr_items_inv_avbl / nbr_items_in_proj
  ))


