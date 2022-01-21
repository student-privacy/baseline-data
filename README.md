# baseline-data
Repo and codebase to aggregate raw CrowdTangle json downloads into an aggregated data set.

Baseline data set for studies of this project.

Version: 1.0 January 2021

For more information on how the JSON data was accessed via the CrowdTangle API, please
refer to [our download and preprocessing repository](https://github.com/student-privacy/download-baseline-data).

*NB Running this pipeline on the full data set of 18 million Facebook posts requires a machine with 32 GB of RAM*

Folder structure:

```
.
├── _targets.R		                        # targets pipeline instruction script
├── R/functions.R		                      # file containing all R code to used in the targets pipeline
├── json/             	                    # all post data as returned from the CrowdTangle API
├── nces-data/             	                    # CSV files of public NCES school and district data
│   ├── nces-info-for-schools.csv        	# NCES School Data 18/19 (masked from repo)
│   ├── nces-info-for-districts.csv        	# NCES District Data 18/19 (masked from repo)
│   └── all-institutional-facebook-urls.csv        	# Instituional Facebook URLs to create crosswalk from NCES to FB data (masked from repo)
├── csv/hand-coded-data-400-august-10th.csv         # File containing hand-coded labels for 400 posts for export (masked from repo)
├── var-overview.Rmd		                        # RMarkdown Notebook to Inspect the Database
├── original-sampling-script.R		                        # Code used to sample images with posts
└── README.md
```