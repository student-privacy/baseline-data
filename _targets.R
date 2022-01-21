library(targets)
library(tarchetypes)

# Source R files
lapply(list.files("./R", full.names = TRUE), source)

# Set target-specific options such as packages.
options(tidyverse.quiet = TRUE)
tar_option_set(packages = c(
  "tidyverse", "data.table", "tidymodels", "xgboost", "tidytext"
))

# Define targets
list(
  # JSON Data ---------------------------------------------------------------------
  tar_target(raw, read_in_json_files()),
  tar_target(json, raw %>% clean_json_data()),

  # NCES Data ---------------------------------------------------------------------
  tar_target(nces_districts_f, here::here('nces-data', 'nces-info-for-districts.csv'), format = "file"),
  tar_target(nces_schools_f, here::here('nces-data', 'nces-info-for-schools.csv'), format = "file"),
  tar_target(nces_all, read_and_prepare_nces_data(nces_districts_f, nces_schools_f)),
  tar_target(nces_preselected, nces_all %>% preselect_nces_variables()),
  tar_target(nces, nces_preselected %>% clean_nces_variables()),

  # Public URLs ---------------------------------------------------------------------
  tar_target(urls_f, here::here('nces-data', 'all-institutional-facebook-urls.csv'), format = "file"),
  tar_target(public_urls, get_and_clean_public_urls(urls_f)),

  # Joining Data ---------------------------------------------------------------------
  tar_target(json_with_nces_ids, attach_nces_ids_to_json(json, public_urls)),
  tar_target(json_with_nces, attach_nces_data_to_json(json_with_nces_ids, nces)),

  # Cleaning and Aggregate Data ------------------------------------------------------
  tar_target(reduced_payload, extract_variables_to_clean(json_with_nces)),
  tar_target(temp_variables, extract_variables_to_not_clean(json_with_nces)),
  tar_target(reduced_payload_clean, reduced_payload %>% clean_and_aggregate_master()),
  tar_target(clean, cbind(reduced_payload_clean, temp_variables)), # MAKE SURE not to alter row arrangement in `clean_and_aggregate_master()`

  ## EXPORT EXAMPLE FOR STUDIES ##
  
  # Export Sampled Data for FB Privacy Study ---------------------------------------------------------------------
  tar_target(sampled_posts_f, here::here('csv', 'hand-coded-data-400-august-10th.csv'), format = 'file'),
  tar_target(sampled_posts, read_csv(sampled_posts_f)),
  tar_target(sampled_clean, clean_hand_coded_sample(sampled_posts)),
  tar_target(sampled_allvars, select_subset(clean, sampled_clean)),

  tar_target(account_n_posts_hashmap, get_n_posts_by_account(clean))#,  # for n_posts variables
  #tar_target(engagement_vars_intercorrelations, get_engagement_intercorrelations(clean))#,
  #tar_target(subset_for_extrapolation, clean %>% filter(shares_photo) %>% select(date, account_subscriber_count, like_count, share_count, comment_count, love_count, wow_count, haha_count, care_count)), # save RAM
)

