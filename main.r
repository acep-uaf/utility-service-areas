library(tidyverse)
library(rvest)
library(glue)
library(sf)
dir.create("data", showWarnings = FALSE)

certificates_csv <- read_csv("rca_electric_certificates.csv") %>%
  filter(`certificate_status` == "Active") #%>% # Filter to active utilities
  # filter(`utility_type` == "Electric") #Filter to electric utilities

download_kml_and_cert <- function(certificate) {
  cert_url <- certificate$cpcn_url
  cert_number <- certificate$certificate_number
  kml_out_path <- file.path("data", glue("{cert_number}-servicearea.kml"))
  cert_out_path <- file.path("data", glue("{cert_number}-certificate.pdf"))
  kml_already_downloaded <- file.exists(kml_out_path)
  cert_already_downloaded <- file.exists(cert_out_path)
  if (kml_already_downloaded && cert_already_downloaded) {
    message(glue("Skipped, certificate {cert_number} was already downloaded")) # message vs print?
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
        message(glue("KML for certificate {cert_number} successfully downloaded"))
      } else {
        warning(glue("Skipping download: KML URL for certificate {cert_number} not found."))
      }
    }
    if (!cert_already_downloaded) {
      cert_url <- page_html %>%
        html_element("a[href*='ViewFile.aspx'][id$='CertDoc']") %>%
        html_attr("href")
      
      if (!is.na(cert_url)) {
        download.file(cert_url, destfile = cert_out_path)
        message(glue("PDF of certificate {cert_number} successfully downloaded"))
      } else {
        warning(glue("Skipping download: PDF URL for certificate {cert_number} not found."))
      }
    }
    # message(glue("Certificate {cert_number} successfully downloaded"))
  }
  
  
  
  
}

for (i in 1:nrow(certificates_csv)) {
  remaining <- nrow(certificates_csv) - i
  message(glue("{remaining} certificates remaining in the download queue"))
  cur_certificate <- certificates_csv[i, ]
  download_kml_and_cert(cur_certificate)
}

read_kml_description <- function(cert_num) {
  kml_path <- glue("data/{cert_num}-servicearea.kml")
  stopifnot(file.exists(kml_path))
  desc <- st_read(kml_path, quiet = TRUE)[1, ]$Description # A few KMLs for Doyon Electric utilities have duplicated description fields (one in HTML for some reason, just get the first one for now). TODO test length and make sure they are identical
  if (startsWith(desc, "<html")) {
    # https://rca.alaska.gov/RCAWeb/Certificate/CertificateDetails.aspx?id=c89cf393-1bf6-483d-a777-e9af4b204710
    # https://www.statology.org/r-find-character-in-string/
    start_index <- unlist(gregexpr("Granted to:", desc))[1]
    end_index <- unlist(gregexpr("</td> </tr> </table> </td> </tr> </table>", desc))[1] - 1
    new_desc <- substr(desc, start_index, end_index) %>%
      gsub("&lt;", "<", .) %>%
      gsub("&gt;", ">", .)
    return(new_desc)
  } else {
    return(desc)
  }
}

safe_read_kml_description <- possibly(read_kml_description, otherwise = NA_character_)

certificates <- certificates_csv %>%
  rowwise() %>%
  mutate(kml_desc_field = safe_read_kml_description(certificate_number)) %>%
  ungroup() %>%
  separate_wider_regex(kml_desc_field, patterns = c(
    "Granted to: ",
    kml_utility_name = "[-A-Za-z/().,& ]+", # TODO: simplify all of these regex patterns
    "(?:<br><br>Utility Type: )?",
    kml_utility_type = "(?:[A-Za-z]+)?",
    "(?:<br>)?<br>CHRONOLOGY: ",
    kml_most_recent_update_included = "[-.,?A-Za-z0-9/():& ]*",
    "(?:<br> ?(?:<br> ?)?)?"
  )) 
