library(tidyverse)
library(rvest)
library(glue)
library(sf)
library(httr)
library(lubridate, warn.conflicts = FALSE)
library(tmap)
dir.create("data", showWarnings = FALSE)

certificates_csv <- read_csv("rca_electric_certificates.csv") %>%
  filter(`certificate_status` == "Active") %>% # Filter to active utilities
  distinct(certificate_number, .keep_all = TRUE) # Quick and dirty way to get rid of duplicate rows that refer to the same certificate that is "co-owned" by two different entities. This just keeps the first row. The certificate url is the same in both so it doesn't matter which one is kept.
# filter(`utility_type` == "Electric") #Filter to electric utilities

# This code adds a new field to certificates_csv - "entity_type" (utility or operator)
# An operator produces electricity and sells all its electric output to a utility or utilities.
# A utility distributes/delivers electricity to the customers who live in its service area. Most utilities in Alaska generate some or all of their own electricity (in other words, not all utilities purchase electricity from operators).
# RCA issues certificates to utilities, and also issues certificates to operators, who sell power to utilities, but don't actually service any households or customers.
# Operators do not have "service areas" and the ones assigned by RCA do not make very much sense. 
# For example, it might be a circle around a wind farm or a random spot in a lake.
# We need to add this field so we can easily distinguish between these two types of entities, enabling us to exclude the psuedo-service areas of operators.
# https://github.com/acep-uaf/utility-service-areas/issues/18

operator_ids <- c(
  # Gustavus Utility Service, Inc., subsidiary of Alaska Power & Telephone Company, owns and operates the Falls Creek hydroelectric facilities.
  # Alaska Power Company (also a subsidiary of Alaska Power & Telephone Company) purchases "all of the electric energy and capacity" of the Falls Creek Power Facilities
  # https://rca.alaska.gov/RCAWeb/ViewFile.aspx?id=6d6ff0db-240d-4fa5-8314-8fd8a9a33588
  # https://docs.publicnow.com/viewDoc?filename=201404%5CEXT%5CE3D47B50365CCC308D7ADD59726CC880820447F4_EEC5F078E9ECE7B508DEF511BE8F31137112704A.PDF
  765,
  # Alaska Industrial Development & Export Authority owns the Snettisham Hydroelectric Project. The project is operated and maintained by Alaska Electric Light and Power Company, who also purchases Snettisham Hydro power
  # https://rca.alaska.gov/RCAWeb/ViewFile.aspx?id=dc64d68e-d639-499e-9d3b-2f6325ba66a1
  # https://rca.alaska.gov/RCAWeb/ViewFile.aspx?id=E7E05BD9-783C-4587-BEAA-C12D590D7741
  549,
  # BBL Hydro, Inc., subsidiary of Alaska Power & Telephone Company, owns and operates the Black Bear Lake hydroelectric project
  # Alaska Power Company purchases power from the project
  # https://lowimpacthydro.org/lihi-certificate-22-black-bear-lake-hydroelectric-project-alaska/
  # https://docs.publicnow.com/viewDoc?filename=201404%5CEXT%5CE3D47B50365CCC308D7ADD59726CC880820447F4_EEC5F078E9ECE7B508DEF511BE8F31137112704A.PDF
  573,
  # Municipality of Anchorage d/b/a Anchorage Hydropower owns 53.33% of the Eklutna Hydroelectric Project
  # Anchorage Hydropower sells all its electric output to Chugach Electric Association (CEA) and Matanuska Electric Association (MEA)
  # https://eklutnahydro.com/
  # https://rca.alaska.gov/RCAWeb/ViewFile.aspx?id=ea36e858-51ee-43e4-9baa-232206f89f31
  # https://www.muni.org/departments/budget/utilitiesenterprise/2021%20utilities/2021%20ppsd%20ent%20util/web%2001%20-%20anchorage%20hydropower%20utility.pdf
  780,
  # Alaska Electric and Energy Cooperative, Inc.
  # "All of [Homer Electric Association]’s power is supplied by Alaska Electric & Energy Cooperative, Inc. (AEEC), a subsidiary of HEA that holds title
  # to substantially all of the transmission lines and substations used to serve HEA’s members and handles wholesale power purchases on behalf of HEA." 
  # https://www.cooperative.com/programs-services/bts/radwind/Documents/RADWIND-Case-Study-Homer-Electric-July-2021.pdf
  # https://www.homerelectric.com/my-cooperative/board-of-directors/alaska-electric-energy-cooperative-aeec/
  640,
  # TDX St. Paul Wind, LLC is a subsidiary of Tanadgusix Corporation (TDX Corp) 
  # TDX "generate[s] and sell[s] wholesale electric power to the City of St. Paul"
  # https://rca.alaska.gov/RCAWeb/ViewFile.aspx?id=197c2346-7987-4d1b-ae44-b76a6632bab1
  749,
  # Goat Lake Hydro, Inc., subsidiary of Alaska Power & Telephone Company, owns and operates the Goat Lake hydroelectric project
  # Alaska Power Company purchases power from the project
  # https://rca.alaska.gov/RCAWeb/ViewFile.aspx?id=5fc44482-1a65-4425-8e09-f432c00b3800
  # https://lowimpacthydro.org/lihi-certificate-26-goat-lake-hydroelectric-project-alaska/
  521,
  # Haida Energy, Inc., a joint venture between Haida Corp and AP&T, owns and operates the Hiilangaay hydroelectric project (formerly named Reynolds Creek)
  # Alaska Power Company purchases power from the project
  # https://aws.state.ak.us/OnlinePublicNotices/Notices/Attachment.aspx?id=117400
  # http://www.haidacorporation.com/haida-energy.html
  760,
  # Alaska Environmental Power, LLC, owns and operates the Delta Wind Farm near Delta Junction, Alaska
  # Golden Valley Electric Association purchases the energy output from the project
  # https://akenergyauthority.org/Portals/0/Programs/Wind/Case%20Studies/DeltaAreaWindTurbines2016.pdf
  # https://www.gvea.com/services/energy/sources-of-power/
  742,
  # Alaska Electric Generation & Transmission Cooperative, Inc
  # It is currently a single member G&T co-op made up of Matanuska Electric Association alone, Homer Electric Association used to be part of it until the creation of AEEC.
  # Chugach used to sell power to AEG&T who sold it to MEA, but that ended in 2015
  # https://www.sec.gov/Archives/edgar/data/878004/000087800415000007/c004-20141231x10k.htm
  # https://rca.alaska.gov/RCAWeb/ViewFile.aspx?id=71325606-5b54-4b3e-8768-4b5ced0135ed
  # https://www.homerelectric.com/wp-content/uploads/AEEC-HEA-History.pdf
  # https://zenodo.org/records/14908275/files/ACEP_Railbelt%20G&T%20History_2025.pdf?download=1
  345,
  # Aurora Energy, LLC owns and operates a coal-fired power plant in Fairbanks
  # Golden Valley Electric Association purchases power from the plant
  # https://fnsb.gov/DocumentCenter/View/1155/Presentation--District-Heating-Aurora-Energy-PDF
  # https://usibelli.com/company/customers
  # https://www.gvea.com/services/energy/sources-of-power/
  520,
  # Aleutian Wind Energy, LLC, subsidiary of TDX Power, owns and operates a wind power project in Sand Point, Alaska
  # TDX Sand Point Generating, LLC, subsidiary of TDX Holdings, purchases wind-power generated electric energy from the project
  # TDX Power and TDX Holdings are subsidiaries of Tanadgusix Corporation
  # https://rca.alaska.gov/RCAWeb/ViewFile.aspx?id=edfd7897-9f53-491b-928c-757dd28efd3d
  735,
  # Kwaan Electric Transmission Intertie Cooperative, Inc.
  # "The HECLA Greens Creek Mine (GCMC) is connected to the Juneau electricity distribution network via overhead and undersea transmission
  # between West Juneau to the mine site on Admiralty Island. The undersea cable and overhead transmission on Admiralty Island are owned and
  # operated by Kwaan Electric Transmission Intertie Cooperative (KWETICO), which receives a wheeling charge for the energy traveling through 
  # their transmission infrastructure to the mine. This extension is intended to one day connect to Hoonah." 
  # "[Alaska Electric Light and Power Company] shall include in its bills to [the mine], and [the mine] shall pay to AELP... transmission charges assessed to AELP by" KWETICO
  # https://juneau.org/wp-content/uploads/2019/03/CBJ-Energy-Strategy-Approved.pdf
  # https://rca.alaska.gov/RCAWeb/ViewFile.aspx?id=277E0D76-E646-4210-BFAA-2E81AADED4FB
  710
)

# Delete rows representing certificates that are inactive but mistakenly are still assigned active status by RCA

inactive_ids <- c(
  # https://github.com/acep-uaf/utility-service-areas/issues/23
  59,
  # https://github.com/acep-uaf/utility-service-areas/issues/15
  71,
  # https://github.com/acep-uaf/utility-service-areas/issues/19
  523
)

certificates_csv <- certificates_csv %>%
  mutate(entity_type = ifelse(
    certificate_number %in% operator_ids,
    "operator",
    "utility")) %>%
  filter(!(certificate_number %in% inactive_ids))

s <- session("https://rca.alaska.gov/RCAWeb/home.aspx")

certs_missing_kml_files <- numeric(length = 0)

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
        certs_missing_kml_files <<- c(certs_missing_kml_files, cert_number) # Append/"combine" new id to the end, ugly global variable operator
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
  # message(glue("Took {end.time.outer - start.time.outer} seconds to run function for certificate {cert_number}."))
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
    # TODO no longer needed
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
  relocate(kml_most_recent_update_date, .after = kml_most_recent_update_included) %>%
  ungroup()

# TODO: warning, kmls for certs_missing_kml_files are missing 

sf_use_s2(TRUE)

file_list <- list.files("data/", pattern = "-servicearea.kml$", full.names = TRUE)
sf_list <- map(file_list, ~st_read(.x, quiet = TRUE))
merged <- bind_rows(sf_list) %>%
  mutate(certificate_number = as.numeric(str_extract(Name, regex("(?!Certificate No. )[\\d]+")))) %>%
  select(-c(Name, Description)) %>%
  mutate(geometry = st_make_valid(geometry)) %>%
  group_by(certificate_number) %>%
  summarise(geometry = st_union(geometry)) %>%
  ungroup() %>%
  # mutate(geometry = st_cast(geometry, "POLYGON")) %>% # Not sure if we should do this. But it's easier to work with (pan/zoom to/highlight) individual polygons than a multipolygon in leaflet. Especially discontiguous ones like AVEC
  inner_join(certificates, by=join_by(certificate_number)) %>%
  select(-geometry,everything()) # Move geometry to end

#sf_use_s2(FALSE)
#tmap_mode("view")
#tm_shape(merged) +
#  tm_polygons(fill_alpha=0.5)

st_write(merged %>% filter(utility_type == "Electric", entity_type == "utility"), glue("test-{as.integer(Sys.time())}.geojson"))
