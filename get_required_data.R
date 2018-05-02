library(mlutils)

all_item <- dataset.load(name = 'myHiveConnector', query = 'select * from omahala.new_item_identification_try_bbq;')

items <- all_item$mds_fam_id

tera_query <- paste0(
  'select item_nbr, avg(on_hand_rtl_amt) as on_hand_rtl_amt ', 
  'from wm_user.item_wkly_inv ',
  'where item_nbr in (', paste0(items, collapse = ', '), ') and ',
  'wm_yr_wk between 11508 and 11708 and ',
  'group by item_nbr;'
)

inv_data <- dataset.load(name = 'smishr2 teradata connector', query = tera_query)
