##### JSON DATA #####

select_and_clean_variable_names <- function(d) {
  return(
    d %>% 
      select(
        matches('date'), 
        matches('updated'), 
        matches('type'), 
        matches('postUrl'), 
        matches('id'), 
        matches('media'),
        matches('account.id'), 
        matches('account.handle'), 
        matches('account.name'),
        matches('account.subscriberCount'), 
        matches('account.url'),
        matches('statistics.actual')
      ) %>% 
      janitor::clean_names()
  )
}

read_in_json_files_url_text <- function() {
  files <- list.files('./json', pattern = '.json', full.names = TRUE)
  #files <- files[1:5] # testing
  n_files <- length(files)
  return(  
    imap_dfr(files, function(file, index) {
    cat("\014Processing", index, "/", n_files, "json files")
    file %>% 
      jsonlite::fromJSON(simplifyDataFrame = TRUE, flatten = TRUE) %>% 
      pluck('result', 'posts') %>% 
      mutate(media =  map(media, pluck, 'type')) %>% 
      select(matches('postUrl|description')) %>% 
      janitor::clean_names()
    }) %>% 
      distinct(post_url, .keep_all = TRUE)
  )
}

read_in_json_files <- function() {
  files <- list.files('./json', pattern = '.json', full.names = TRUE)
  #files <- files[1:5] # testing
  n_files <- length(files)
  return(  
    imap_dfr(files, function(file, index) {
      cat("\014Processing", index, "/", n_files, "json files")
      file %>% 
        jsonlite::fromJSON(simplifyDataFrame = TRUE, flatten = TRUE) %>% 
        pluck('result', 'posts') %>% 
        mutate(media =  map(media, pluck, 'type')) %>% 
        select_and_clean_variable_names()
    }) %>% 
      distinct(post_url, .keep_all = TRUE)
  )
}

preselect_json_variables <- function(d) {
  return(
    d %>% 
      select(
        date, updated, type, post_url, media,
        account_handle, account_platform_id, account_subscriber_count,
        matches('statistics')
      ) 
  )
}

rename_json_variables <- function(d) {
  return(
    d %>% 
      rename(
        user_name = account_handle,
        facebook_id = account_platform_id
      ) %>% 
      (function(dat){
        names(dat) <- names(dat) %>% str_remove('statistics_actual_')
        return(dat)
      })
  )
}

clean_json_variables <- function(d) {
  return(
    d %>% 
      mutate(date = as.POSIXct(date, tz = 'UTC'))
  )
}

clean_json_data <- function(d) {
  return(
    d %>% 
      preselect_json_variables() %>% 
      rename_json_variables() %>% 
      clean_json_variables() %>% 
      data.table()
  )
}

##### NCES DATA #####

read_and_prepare_nces_data <- function(nces_districts_f, nces_schools_f) {
  
  ## NCES disctricts
  
  d <- read.csv(nces_districts_f, fill=T, encoding = "UTF-8") %>% 
    janitor::clean_names()
  
  # Get Facebook ID and User name of districts
  
  d$link_proc[is.na(d$link_proc)] <- ""
  
  d$district_profile_id <- NA
  
  d$district_facebook_id <- d$link_proc %>% 
    sapply(., function(x) { if (str_detect(x, "[[:digit:]]{9,1000}")) { return( str_extract(x, "[[:digit:]]{9,1000}") ) } else {return(NA)} }) %>%  # extract fb ids if possible
    str_replace_all("([.])|[[:punct:]]", "\\1")   # finally, punctuation (except dots) for final matching, result is either fb id or user_name
  
  d$district_user_name <- d$link_proc %>% 
    str_remove_all("[[:digit:]]{9,}") %>%
    str_replace_all("([.])|[[:punct:]]", "\\1")    # the only punctation in facebook user names is a dot, all other punctation can be cleaned
  
  d <- d %>% select(-link_proc, -proc_link)
  
  # Disambiguate names
  
  names(d)[grep("nces_id", names(d))] <- "district_nces_id"
  names(d)[grep("link", names(d))] <- "district_facebook_url"
  
  # NAs and remove non-UTF-8 chars
  
  bad_char <- d$location_address_2_district_2017_18[1]  # hacked, just at 1st position for this file!
  d[d==bad_char] <- NA
  d[d==""] <- NA; d[d=="    "] <- NA
  d[d=="-"] <- NA; d[d=="–"] <- NA; d[d=="–"] <- NA
  
  d <- d %>% mutate_if(is.character, ~gsub('[^ -~]', '', .))
  
  # A lot of entries start with "="
  
  d <- lapply(d, gsub, pattern='=', replacement='') %>% as.data.frame()
  
  # Exclude quotation marks
  
  d <- lapply(d, str_remove_all, pattern='[\"\']') %>% as.data.frame()
  
  # Put primary key as first column
  
  d <- d %>% select(district_nces_id, everything())
  
  nces_districts <- d %>% tibble()
  
  ## NCES schools
  
  d <- read.csv(nces_schools_f, fill=T, encoding = "UTF-8") %>% 
    janitor::clean_names() 
  
  bad_char <- d$location_address_2_public_school_2018_19[1]  # hacked, just at 1st position for this file!
  d[d==bad_char] <- NA
  d[d==""] <- NA
  d[d=="    "] <- NA
  d[d=="-"] <- NA
  d[d=="–"] <- NA
  d[d=="–"] <- NA
  
  d <- d %>%
    mutate_if(is.character, ~gsub('[^ -~]', '', .))  # Remove non-UTF-8 chars
  
  # A lot of entries start with "="
  
  d <- lapply(d, gsub, pattern='=', replacement='') %>% as.data.frame()
  
  # Exclude quotation marks
  
  d <- lapply(d, str_remove_all, pattern='[\"\']') %>% as.data.frame()
  
  nces_schools <- d %>% tibble()
  
  return(list(schools = data.table(nces_schools), 
              districts = data.table(nces_districts)))
}

preselect_nces_variables <- function(l) {
  #l$schools %>% names()
  l$schools <- l$schools %>% 
    select(
      school_id_nces_assigned_public_school_latest_available_year,
      charter_authorizer_state_id_primary_public_school_2018_19,
      congressional_code_public_school_2018_19,
      state_name_public_school_latest_available_year,
      total_students_all_grades_excludes_ae_public_school_2018_19,
      national_school_lunch_program_public_school_2018_19,
      free_and_reduced_lunch_students_public_school_2018_19,
      american_indian_alaska_native_students_public_school_2018_19,
      asian_or_asian_pacific_islander_students_public_school_2018_19,
      hispanic_students_public_school_2018_19,
      black_students_public_school_2018_19,
      white_students_public_school_2018_19,
      hawaiian_nat_pacific_isl_students_public_school_2018_19,
      two_or_more_races_students_public_school_2018_19,
      total_race_ethnicity_public_school_2018_19,
      lowest_grade_offered_public_school_2018_19,
      highest_grade_offered_public_school_2018_19,
      urban_centric_locale_public_school_2018_19,
      virtual_school_status_sy_2018_19_onward_public_school_2018_19,
      agency_type_district_2018_19
    )
  #l$districts %>% names()
  l$districts <- l$districts %>% 
    select(
      district_nces_id,
      metro_micro_area_code_district_2017_18,
      congressional_code_district_2017_18,
      state,
      total_number_operational_schools_public_school_2017_18,
      total_students_all_grades_excludes_ae_district_2017_18,
      free_and_reduced_lunch_students_public_school_2017_18,
      american_indian_alaska_native_students_district_2017_18,
      asian_or_asian_pacific_islander_students_district_2017_18,
      hispanic_students_district_2017_18,
      black_students_district_2017_18,
      white_students_district_2017_18,
      hawaiian_nat_pacific_isl_students_district_2017_18,
      two_or_more_races_students_district_2017_18,
      pupil_teacher_ratio_district_2017_18,
      agency_type_district_2017_18,
      urban_centric_locale_district_2017_18,
      lea_charter_status_district_2017_18
    )
  return(l)
}

clean_nces_variables <- function(l) {
  
   # May be added later
  
  return(l)
}
  
##### PUBLIC SCHOOL URL DATA #####

clean_public_url <- function(strings) {
  return(
    strings %>%
      sapply(., utils::URLdecode) %>%  
      tolower() %>%
      str_remove_all("^http[s]?://") %>%   # optional transfer protocol specification at beginning
      str_remove_all("^w{3,}.") %>%  # optional www. at beginning
      str_replace_all("/{1,}","/") %>%   # redundant forward slashes
      str_remove_all("facebook.[a-z]{2,3}/") %>%  # international endings, .de, .fr, .it, ...
      str_remove_all("posts/.*|videos/.*|timeline.*|events/.*") %>% # content chunks
      str_remove_all("/$") %>%    # forward slashes at the end of URLs
      str_remove_all("pages/|pg/|hashtag/|people/") %>%  # old page identifyers
      str_remove_all("category/(?=\\S*['-])([a-zA-Z'-]+)/")  # category names with dashes
  )
}

get_and_clean_public_urls <- function(urls_f) {
  
  d_wp <- read_csv(urls_f, col_types = cols(nces_id = col_character()))
  
  d_wp$url_clean <- d_wp$url %>% clean_public_url()
  
  d_wp$facebook_id <- d_wp$url_clean %>% 
    sapply(., function(x) { if (str_detect(x, "[[:digit:]]{9,1000}")) { return( str_extract(x, "[[:digit:]]{9,1000}") ) } else {return(NA)} }) %>%  # extract fb ids if possible
    str_replace_all("([.])|[[:punct:]]", "\\1")   # finally, punctuation (except dots) for final matching, result is either fb id or user_name
  
  d_wp$user_name <- d_wp$url_clean %>% 
    str_remove_all("[[:digit:]]{9,}") %>%
    str_replace_all("([.])|[[:punct:]]", "\\1")    # the only punctation in facebook user names is a dot, all other punctation can be cleaned
  
  d <- d_wp %>% select(user_name, facebook_id, nces_id)
  
  d[d==""] <- NA
  
  return(d %>% select(facebook_id, user_name, nces_id) %>% data.table())
}

##### JOIN DATA #####

attach_nces_ids_to_json <- function(json, public_urls) {
  json$nces_id <- NA
  
  # First, join by account handle
  # sum(tolower(raw$account_handle) %in% tolower(public_urls$user_name)) # 15M
  index_of_a_in_b <- match(tolower(json$user_name), tolower(public_urls$user_name)) # Caution: Takes first if multiple matches
  json$nces_id <- public_urls$nces_id[index_of_a_in_b]
  
  # Then, try to fill remaining positions through Facebook ID matches
  index_of_a_in_b2 <- match(tolower(json$facebook_id), tolower(public_urls$facebook_id))
  index_of_a_in_b2[!is.na(json$nces_id)] <- NA # we already assigned those
  json$nces_id2 <- public_urls$nces_id[index_of_a_in_b2]
  json$nces_id <- coalesce(json$nces_id, json$nces_id2)
  json <- json %>% select(-nces_id2)
  
  #json$nces_id %>% is.na %>% sum # only 35,695 posts have no assigned NCES ID!

  return(json)
}

get_nces_school_id <- function(x) {
  # The first 7 digits of the 12 digit school ID are the district ID, the last five are the school ID, put together, they make a 12 digit unique code for each school.
  if (is.na(x))
    return(NA)
  if (nchar(x)>7) # if it is a school ID:
    return(substr(x, nchar(x)-5+1, nchar(x)))
  else
    return(NA)
}

get_nces_district_id <- function(x) {
  # The first 7 digits of the 12 digit school ID are the district ID, the last five are the school ID, put together, they make a 12 digit unique code for each school.
  if (is.na(x))
    return(NA)
  if (nchar(x)>7) # if it is a school ID:
    return(substr(x, 1, 7))
  else
    return(x)
}

get_is_school <- function(x) {
  if (is.na(x))
    return(FALSE)
  if (nchar(x)>7) # if it is a school ID:
    return(TRUE)
  else
    return(FALSE)
}

get_is_district <- function(x) {
  if (is.na(x))
    return(FALSE)
  if (nchar(x)>7) # if it is a school ID:
    return(FALSE)
  else
    return(TRUE)
}

attach_nces_data_to_json <- function(json, nces) {
  
  json$is_school <- map_lgl(json$nces_id, get_is_school)
  json$is_district <- map_lgl(json$nces_id, get_is_district) # cant just !inverse because of a few NAs
  
  json$has_school_data <- json$is_school
  json$has_district_data <- (json$is_school | json$is_district)
  
  # First, try to join school data, which already matches ~ 5M posts to NCES data
  schools <- nces$schools
  schools$nces_id_school <- map_chr(schools$school_id_nces_assigned_public_school_latest_available_year, get_nces_school_id)
  json$nces_id_school <- map_chr(json$nces_id, get_nces_school_id)
  index_of_a_in_b_s <- match(json$nces_id_school, schools$nces_id_school) 
  
  # Then, also create a match with district data, which also leads to around 13M matches
  districts <- nces$districts
  json$nces_id_district <- map_chr(json$nces_id, get_nces_district_id)
  index_of_a_in_b_d <- match(json$nces_id_district, districts$district_nces_id) 
  
  # Together, there are only 54,055 posts with with neither NCES school nor NCES district data (.3%)
  #coalesce(index_of_a_in_b_s, index_of_a_in_b_d) %>% is.na() %>% sum()
  
  # Rename variables and cbind all data 
  schools <- schools %>% 
    rename(
      nces_id = school_id_nces_assigned_public_school_latest_available_year,
      charter_authorizer_state_id = charter_authorizer_state_id_primary_public_school_2018_19,
      congressional_code = congressional_code_public_school_2018_19,
      state = state_name_public_school_latest_available_year,
      school_nat_lunch_program = national_school_lunch_program_public_school_2018_19,
      n_students_free_reduced_lunch = free_and_reduced_lunch_students_public_school_2018_19,
      n_students_native = american_indian_alaska_native_students_public_school_2018_19,
      n_students_asian = asian_or_asian_pacific_islander_students_public_school_2018_19,
      n_students_black = black_students_public_school_2018_19,
      n_students_white = white_students_public_school_2018_19,
      n_students_hispanic = hispanic_students_public_school_2018_19,
      n_students_hawaiian = hawaiian_nat_pacific_isl_students_public_school_2018_19,
      n_students_multirace = two_or_more_races_students_public_school_2018_19,
      n_students_total_ethnicity = total_race_ethnicity_public_school_2018_19,
      n_students = total_students_all_grades_excludes_ae_public_school_2018_19,
      n_reduced_free_lunch = free_and_reduced_lunch_students_public_school_2018_19,
      lowest_grade = lowest_grade_offered_public_school_2018_19,
      highest_grade = highest_grade_offered_public_school_2018_19
    ) %>% 
    (function(d){names(d)<-paste0('schools_',names(d));return(d)})
  
  districts <- districts %>% 
    rename(
      nces_id = district_nces_id,
      area_code = metro_micro_area_code_district_2017_18,
      congressional_code = congressional_code_district_2017_18,
      n_students_free_reduced_lunch = free_and_reduced_lunch_students_public_school_2017_18,
      n_students_native = american_indian_alaska_native_students_district_2017_18,
      n_students_asian = asian_or_asian_pacific_islander_students_district_2017_18,
      n_students_black = black_students_district_2017_18,
      n_students_white = white_students_district_2017_18,
      n_students_hispanic = hispanic_students_district_2017_18,
      n_students_hawaiian = hawaiian_nat_pacific_isl_students_district_2017_18,
      n_students_multirace = two_or_more_races_students_district_2017_18,
      n_schools = total_number_operational_schools_public_school_2017_18,
      n_students = total_students_all_grades_excludes_ae_district_2017_18,
      pupil_teacher_ratio = pupil_teacher_ratio_district_2017_18,
    ) %>% 
    (function(d){names(d)<-paste0('districts_',names(d));return(d)})
  
  combined <- cbind(json, schools[index_of_a_in_b_s,], districts[index_of_a_in_b_d,])
  
  return(combined)
}

##### CLEANING #####

remove_redundant_variables <- function(d) {
  return(
    d %>% 
      select(-schools_nces_id, -districts_nces_id)
  )
}

media_type_clean <- function(d) {
  cat('Cleaning post and media type data...\n')
  # map(d$media, length) %>% unlist %>% table
  # There are up to two distinct media types per post
  d <- d %>% 
    mutate(
      media_1 = map_chr(media, function(x){if(is.null(pluck(x,1))){return(NA)}else{return(pluck(x,1))}}),
      media_2 = map_chr(media, function(x){if(is.null(pluck(x,2))){return(NA)}else{return(pluck(x,2))}})
    ) %>% 
    mutate(
      type = case_when(
        type == 'photo' ~ 'photo',
        type == 'album' ~ 'photo',
        type == 'status' ~ 'post', # regular post without media nor links
        type == 'link' ~ 'link',
        type == 'native_video' ~ 'video',
        type == 'youtube' ~ 'video',
        type == 'live_video_complete' ~ 'video',
        type == 'live_video' ~ 'video',
        type == 'vine' ~ 'video',
        TRUE ~ 'other'
      )
    ) %>% 
    rename(post_type = type) %>% 
    mutate(
      shares_photo = case_when(
        media_1 == 'photo' | media_2 == 'photo' | post_type == 'photo' ~ TRUE,
        TRUE ~ FALSE
      ),
      shares_video = case_when(
        media_1 == 'video' | media_2 == 'video' | post_type == 'video' ~ TRUE, # Note: YT is regarded as video here
        TRUE ~ FALSE
      ),
      shares_link = case_when(
        post_type == 'link' ~ TRUE,
        TRUE ~ FALSE
      )
    ) %>% 
    select(-post_type, -media, -media_1, -media_2)
  return(d)
}

state_clean <- function(d) {
  cat('Cleaning state data...\n')
  # We can either use the state from district or school data -> coalesce to one var
  d <- d %>% 
    mutate(
      schools_state = tolower(schools_state),
      districts_state = tolower(districts_state)
    ) %>% 
    mutate(state = coalesce(schools_state, districts_state)) %>% 
    select(-schools_state, -districts_state)
  return(d)
}

variable_types_clean <- function(d) {
  
  d <- d %>% 
    mutate(across(matches('n_students_|n_students$'), as.integer)) %>% 
    mutate(schools_n_students_native = as.integer(schools_n_students_native)) %>% 
    mutate(schools_n_reduced_free_lunch = as.integer(schools_n_reduced_free_lunch)) %>% 
    mutate(districts_n_schools = as.integer(districts_n_schools)) %>% 
    mutate(districts_pupil_teacher_ratio = as.numeric(districts_pupil_teacher_ratio)) %>% 
    mutate(schools_school_nat_lunch_program = case_when(
      str_detect(schools_school_nat_lunch_program, '^Yes') ~ TRUE, # CB: There are different kinds of levels, so information is lost here
      schools_school_nat_lunch_program == 'No' ~ FALSE,
      TRUE ~ NA
    ))

  return(d)
}

clean_and_aggregate_master <- function(d) {
  return(
    d %>% 
      remove_redundant_variables() %>% 
      media_type_clean() %>% 
      state_clean() %>% 
      variable_types_clean()
  )
}

extract_variables_to_clean <- function(d) {
  d <- d %>% 
    select(
      schools_nces_id,
      districts_nces_id,
      media,
      type,
      schools_state,
      districts_state,
      schools_school_nat_lunch_program,
      matches('n_students_|n_students$'),
      schools_n_students_native,
      schools_n_reduced_free_lunch,
      districts_n_schools,
      districts_pupil_teacher_ratio
   )
  return(d)
}

extract_variables_to_not_clean <- function(d) {
  names_to_exclude <- d %>% 
    select(
      schools_nces_id,
      districts_nces_id,
      media,
      type,
      schools_state,
      districts_state,
      schools_school_nat_lunch_program,
      matches('n_students_|n_students$'),
      schools_n_students_native,
      schools_n_reduced_free_lunch,
      districts_n_schools,
      districts_pupil_teacher_ratio
    ) %>% 
    names()
  names_to_include <- setdiff(names(d), names_to_exclude)
  d <- d %>% 
    select(!!names_to_include)
  return(d)
}

##### SAMPLED POSTS #####

clean_hand_coded_sample <- function(d) {
  
  d <- d %>% 
    janitor::clean_names() %>% 
    filter(str_detect(url, '^http')) %>%  # 2 rows had 'current totals'
    filter(!is.na(shared_post)) %>% # sample those posts that currently are coded
    select(
      url, ends_with('_highlight'),
      matches('first_names'), 
      matches('last_names'),
      matches('faces_in_image|faces_in_post'),
      matches('faces_connected')
    ) %>% 
    rename(
      faces_connected_students = how_many_names_faces_connected,
      faces_connected_staff = how_many_names_faces_connected_1,
      faces_connected_community = how_many_names_faces_connected_2,
      number_of_community_faces_in_image = number_of_community_faces_in_post
    )
    
  return(d)
}

select_subset <- function(d, sample) {
  
  subset <- d[d$post_url %in% sample$url,]
  
  res <- subset %>% 
    rename(url = post_url) %>% 
    left_join(sample, by = 'url')
  
  return(res)
}

get_n_posts_by_account <- function(dat) {
  
  true_dist_posts <- dat %>% 
    filter(is_district) %>% 
    group_by(nces_id_district) %>% 
    summarise(true_district_posts = n()) %>% 
    ungroup()
  
  dist_posts <- dat %>% 
    group_by(nces_id_district) %>% 
    summarise(district_posts = n()) %>% 
    ungroup()
  
  res <- full_join(true_dist_posts, dist_posts, by='nces_id_district')
  
  return(res)
}

