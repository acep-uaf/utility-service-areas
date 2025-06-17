library(tidyverse)
library(rvest)
library(glue)
library(sf)
library(httr)
library(lubridate, warn.conflicts = FALSE)
dir.create("data", showWarnings = FALSE)

certificates_csv <- read_csv("rca_electric_certificates.csv") %>%
  filter(`certificate_status` == "Active") #%>% # Filter to active utilities
# filter(`utility_type` == "Electric") #Filter to electric utilities

s <- session("https://rca.alaska.gov/RCAWeb/home.aspx")

download_kml_cert_and_chronology <- function(certificate) {
  start.time.outer <- Sys.time()
  cert_url <- certificate$cpcn_url
  cert_number <- certificate$certificate_number
  kml_out_path <- file.path("data", glue("{cert_number}-servicearea.kml"))
  cert_out_path <- file.path("data", glue("{cert_number}-certificate.pdf"))
  chronology_out_path <- file.path("data",
                                   glue("{cert_number}-certificate-chronology.html"))
  kml_already_downloaded <- file.exists(kml_out_path)
  cert_already_downloaded <- file.exists(cert_out_path)
  chronology_already_downloaded <- file.exists(chronology_out_path)
  if (kml_already_downloaded &&
      cert_already_downloaded && chronology_already_downloaded) {
    message(glue("Skipped, certificate {cert_number} was already downloaded")) # message vs print?
  } else {
    s <- s %>%
      session_jump_to(cert_url)
    page_html <- s %>%
      read_html()
    if (!kml_already_downloaded) {
      # Get HTML of certificate page
      # https://r4ds.hadley.nz/webscraping.html#find-elements
      kml_url <- page_html %>%
        html_element("a[href*='ViewFile.aspx'][id$='KMLDoc']") %>%
        html_attr("href")
      
      if (!is.na(kml_url)) {
        download.file(kml_url, destfile = kml_out_path)
        message(glue(
          "KML for certificate {cert_number} successfully downloaded"
        ))
      } else {
        warning(glue(
          "Skipping download: KML URL for certificate {cert_number} not found."
        ))
      }
    }
    if (!cert_already_downloaded) {
      cert_pdf_url <- page_html %>%
        html_element("a[href*='ViewFile.aspx'][id$='CertDoc']") %>%
        html_attr("href")
      
      if (!is.na(cert_pdf_url)) {
        download.file(cert_pdf_url, destfile = cert_out_path)
        message(glue(
          "PDF of certificate {cert_number} successfully downloaded"
        ))
      } else {
        warning(glue(
          "Skipping download: PDF URL for certificate {cert_number} not found."
        ))
      }
    }
    if (!chronology_already_downloaded) {
      start.time <- Sys.time()
      form <- html_form(s)[[1]]
      # or
      # field_values <- lapply(form$fields, function(x) x$value) %>%
      # set_names(map_chr(form$fields, "name"))
      # Because __EVENTTARGET doesn't get detected as a field by rvest we can't use html_form_set/html_form_submit, instead we have to use httr
      field_values <- map(form$fields, "value") %>%
        set_names(map_chr(form$fields, "name"))
      field_values[["__EVENTTARGET"]] <- "PortalPageControl1$_ctl6$PortalPageControl1$TabLink1"
      session_id = (cookies(s) %>%
                      filter(name == "ASP.NET_SessionId"))[1, ]$value
      response <- POST(
        url = cert_url,
        body = field_values,
        encode = "form",
        set_cookies(`ASP.NET_SessionId` = session_id)
        # verbose()
      ) %>% read_html()
      if (!is.na(html_element(response, "a.activePortalTabLink[id$='TabLink1']"))) {
        xml2::write_xml(response, chronology_out_path)
        end.time <- Sys.time()
        message(glue("Took {end.time - start.time} seconds to get chronology for certificate {cert_number}."))
      } else {
        warning(glue(
          "Skipping download: Error loading chronology page for certificate {cert_number}."
        ))
      }
    }
  }
  end.time.outer <- Sys.time()
  message(glue("Took {end.time.outer - start.time.outer} seconds to run function for certificate {cert_number}."))
}

for (i in 1:nrow(certificates_csv)) {
  remaining <- nrow(certificates_csv) - i
  message(glue("{remaining} certificates remaining in the download queue"))
  cur_certificate <- certificates_csv[i, ]
  download_kml_cert_and_chronology(cur_certificate)
}

read_kml_description <- function(cert_num) {
  kml_path <- glue("data/{cert_num}-servicearea.kml")
  stopifnot(file.exists(kml_path))
  desc <- st_read(kml_path, quiet = TRUE)[1, ]$Description # A few KMLs for Doyon Electric utilities have duplicated description fields (one in HTML for some reason, just get the first one for now). TODO test length of duplicates to make sure they are identical. Low priority since this only happens once or twice.
  if (startsWith(desc, "<html")) {
    # https://rca.alaska.gov/RCAWeb/Certificate/CertificateDetails.aspx?id=c89cf393-1bf6-483d-a777-e9af4b204710
    # https://www.statology.org/r-find-character-in-string/
    # TODO: change to stringr
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

read_chronology_table <- function(cert_num) {
  chronology_path <- glue("data/{cert_num}-certificate-chronology.html")
  stopifnot(file.exists(chronology_path))
  chronology_table <- read_html(chronology_path) %>%
    html_element("table.RCAGrid") %>%
    html_table() # Table is already sorted with most recent events first
  if (cert_num == 13) {
    # Two entries in GVEA's chronology table are missing dates. We will add them back manually.
    # https://rca.alaska.gov/RCAWeb/Dockets/DocketDetails.aspx?id=7CC1B7AC-C9BB-4170-A9DB-28310A46DAF8
    chronology_table <- chronology_table %>%
      mutate(`Order Date` = ifelse(Order == "5E", "10/04/2012", `Order Date`)) %>%
      mutate(`Order Date` = ifelse(Order == "5EE", "04/09/2013", `Order Date`)) 
  }
  chronology_table <- chronology_table %>%
    mutate(`Order Date` = if_else(`Order Date` == "", "1/1/1900", `Order Date`)) # TODO: fix this bad solution.
    # Not sure if this is good to do or not, but missing date should not be sorted first/newest
  return(chronology_table)
}

certificates.chronology <- data.frame()

for (i in 1:nrow(certificates_csv)) {
  cur_certificate_number <- certificates_csv[i, ]$certificate_number
  if (i > 1 && cur_certificate_number == certificates_csv[i-1, ]$certificate_number) {
    # Some utilities have two rows in the CSV. Until that is fixed, skip adding the same chronology table twice.
    next
  }
  certificates.chronology <- certificates.chronology %>% 
    rbind(read_chronology_table(cur_certificate_number) %>%
            mutate(`Order Date` = mdy(`Order Date`)) %>%
            arrange(`Order Date`) %>%
            mutate(Certificate = cur_certificate_number) %>%
            select(c(Certificate, `Docket Number`, Order, `Order Date`), everything()))
}

convert_two_digit_years <- function(x, year=1963) {
  # The oldest year appearing in RCA chronologies is 1964
  # https://stackoverflow.com/questions/12323693/convert-two-digit-years-to-four-digit-years-with-correct-century/12957909#12957909
  m <- year(x) %% 100
  year(x) <- ifelse(m > year %% 100, 1900+m, 2000+m)
  return(x)
}

kml_has_newest_service_area_updates <- function(cert_num, kml_update_date) {
  chronology_entries <- certificates.chronology %>%
    filter(Certificate == cert_num)
  if ((chronology_entries %>% tail(n=1))$`Order Date` < kml_update_date) {
    # If the KML has a newer date than the newest entry in the chronology, this means the chronology on RCA's site is incomplete and missing an entry.
    # We will have to manually check the RCA site in this scenario. 
    warning(glue("Certificate {cert_num}'s chronology should have an entry dated {kml_update_date} but doesn't. Maybe it can be found elsewhere on the RCA site."))
    return(NA)
  }
  newer_service_area_changes <- chronology_entries %>%
    filter(`Order Date` > kml_update_date) %>%
    filter(!Type == "Deregulated", !Type == "Controlling Interest")
  if (nrow(newer_service_area_changes) > 0) {
    return(FALSE) # False, there are updates newer than the KML in the certificate chronology
  } else {
    return(TRUE) # True, the KML has the newest updates
  }
}

certificates <- certificates_csv %>%
  rowwise() %>%
  mutate(kml_desc_field = safe_read_kml_description(certificate_number)) %>%
  ungroup() %>%
  separate_wider_regex(
    kml_desc_field,
    patterns = c(
      "Granted to: ",
      kml_utility_name = "[-A-Za-z/().,& ]+",
      # TODO: simplify all of these regex patterns
      "(?:<br><br>Utility Type: )?",
      kml_utility_type = "(?:[A-Za-z]+)?",
      "(?:<br>)?<br>CHRONOLOGY: ",
      kml_most_recent_update_included = "[-.,?A-Za-z0-9/():& ]*",
      "(?:<br> ?(?:<br> ?)?)?"
    )
  ) %>%
  mutate(name_match = tolower(certificate_name) == tolower(kml_utility_name)) %>%
  rename(alt_name = kml_utility_name) %>%
  mutate(alt_name = ifelse(name_match, NA_character_, alt_name)) %>%
  select(-c(name_match, kml_utility_type)) %>% # Don't really need these right now
  rowwise() %>%
  mutate(certificate_last_update_date = (certificates.chronology %>% 
                                           filter(Certificate == certificate_number) %>% 
                                           tail(n = 1))$`Order Date`) %>%
           #format('%m/%d/%y')) %>% # Temporarily change to MM/DD/YY so we don't get issues like xx/xx/64 (in KML descriptions) being converted to 2064-xx-xx. This will break in a few decades though. Fixed by convert_two_digit_years function
  mutate(certificate_last_update_type = (certificates.chronology %>% 
                                           filter(Certificate == certificate_number) %>% 
                                           tail(n = 1))$Type) %>%
  mutate(certificate_last_update_description = (certificates.chronology %>% 
                                                  filter(Certificate == certificate_number) %>% 
                                                  tail(n = 1))$Comment) %>%
  mutate(kml_most_recent_update_date = str_extract(kml_most_recent_update_included, "[\\d]{1,2}\\/[\\d]{2}\\/([\\d]{4}|[\\d]{2})") %>%
           mdy() %>%
           convert_two_digit_years()) %>%
           #format('%m/%d/%y')) %>%
  mutate(kml_has_latest_certificate_update = ifelse(
    is.na(kml_most_recent_update_date), 
    NA,
    kml_has_newest_service_area_updates(certificate_number, kml_most_recent_update_date))) %>%
  ungroup()
   # TODO: remove duplicates. Should just remove them at beginning when reading from csv
