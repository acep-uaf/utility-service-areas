library(tidyverse)
library(rvest)
library(glue)
dir.create("data", showWarnings = FALSE)

certificates_csv <- read_csv("rca_electric_certificates.csv") %>%
  filter(`certificate_status` == "Active")  # Filter to active utilities

download_kml_and_cert <- function(certificate) {
  cert_url <- certificate$cpcn_url
  cert_number <- certificate$certificate_number
  kml_out_path <- file.path("data", glue("{cert_number}-servicearea.kml"))
  cert_out_path <- file.path("data", glue("{cert_number}-certificate.pdf"))
  kml_already_downloaded <- file.exists(kml_out_path)
  cert_already_downloaded <- file.exists(cert_out_path)
  if (kml_already_downloaded && cert_already_downloaded) {
    print(glue("Skipped, certificate {cert_number} was already downloaded"))
  } else {
    page_html <- read_html(cert_url)
    if (!kml_already_downloaded) {
      # Get HTML of certificate page
      # https://r4ds.hadley.nz/webscraping.html#find-elements
      kml_url <- page_html %>%
        html_element("a[href*='ViewFile.aspx'][id$='KMLDoc']") %>%
        html_attr("href")
      
      if (!is.na(kml_url)) {
        download.file(kml_url, destfile = kml_out_path)
      } else {
        message("Skipping download: KML URL not found.")
      }
    }
    if (!cert_already_downloaded) {
      cert_url <- page_html %>%
        html_element("a[href*='ViewFile.aspx'][id$='CertDoc']") %>%
        html_attr("href")
      
      if (!is.na(cert_url)) {
        download.file(cert_url, destfile = cert_out_path)
      } else {
        message("Skipping download: Certificate URL not found.")
      }
    }
    print(glue("Certificate {cert_number} successfully downloaded"))
  }
  
  
  
  
}

for (i in 1:nrow(certificates_csv)) {
  remaining <- nrow(certificates_csv) - i
  print(glue("{remaining} certificates remaining in the download queue"))
  cur_certificate <- certificates_csv[i, ]
  download_kml_and_cert(cur_certificate)
}
