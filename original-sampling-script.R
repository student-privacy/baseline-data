set.seed(909)

# From 100 responses, sample 10 pictures
# These responses are files of 10,000 downloaded Facebook posts (API requests) download chronologically based on a list of Facebook accounts
# While it is not perfectly clear how Facebook's API returns API requests, posts were downloaded chronologically, based on a time frames

rm(list=ls())

unlist_keep_null <- function(l) {
  l[sapply(l, is.null)] <- NA
  unlist(l)
}

fns <- sample(dir("./json/", pattern="*.json", full.names=T), 100, replace=F)
res <- c(); i<-0

for (f in fns) 
{
  j <- rjson::fromJSON(file=f)$result$posts
  
  urls <- cbind(
    map(j, "date") %>% unlist_keep_null(),
    map(j, c("account", "id")) %>% unlist_keep_null(),
    map(j, "type") %>% unlist_keep_null(),
    map(j, c("media", function(i) i[[1]], "url")) %>% unlist_keep_null(),
    map(j, "postUrl") %>% unlist_keep_null()
  ) %>%
    as.data.frame() %>%
    magrittr::set_colnames(c("date", "user", "type", "image_url", "post_url")) %>%
    filter(type=="photo") %>% 
    select(post_url) %>%
    unlist() %>%
    sample(10, replace=F)
  
  res <- c(res, urls)
  
  i <- i+1; cat("\014", i, "% done\n")
}

sink("./photos/urls.txt")
for (url in res)
  cat(url, "\n")
sink()
