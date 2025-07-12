library(tidyverse)
library(rvest)
library(glue)
library(sf)
library(httr)
library(lubridate, warn.conflicts = FALSE)
library(tmap)
dir.create("data", showWarnings = FALSE)

certificates_csv <- read_csv("rca_electric_certificates.csv") %>%
  # filter(`certificate_status` == "Active") %>% # Filter to active utilities
  distinct(certificate_number, .keep_all = TRUE) # Quick and dirty way to get rid of duplicate rows that refer to the same certificate that is "co-owned" by two different entities. This just keeps the first row. The certificate url is the same in both so it doesn't matter which one is kept.
  # filter(`utility_type` == "Electric") #Filter to electric utilities

# TODO in future: check # of entries on live RCA site, make sure matches input csv

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
  message(glue("Took {end.time.outer - start.time.outer} seconds to run function for certificate {cert_number}."))
}

for (i in 1:nrow(certificates_csv)) {
  remaining <- nrow(certificates_csv) - i
  message(glue("{remaining} certificates remaining in the download queue"))
  cur_certificate <- certificates_csv[i, ]
  download_kml_cert_and_chronology(cur_certificate)
}

sf_use_s2(TRUE)

file_list <- list.files("data", pattern = "-servicearea.kml$", full.names = TRUE)
sf_list <- map(file_list, ~st_read(.x, quiet = TRUE))
# Just merge all service areas, active and inactive, electric, natural gas, heat, or otherwise. No manual filtering/patching.
# It's pretty common for inactive service areas, especially old/obscure ones, to not have kmls. So this "raw" layer will still have less rows than the total # of utilities.
merged_raw <- bind_rows(sf_list) %>%
  mutate(certificate_number = as.numeric(str_extract(Name, regex("(?!Certificate No. )[\\d]+(\\.[\\d]+)?")))) %>% # Regex needs to match CPCN "18.1"
  select(-c(Name, Description)) %>%
  mutate(geometry = st_make_valid(geometry)) %>%
  group_by(certificate_number) %>%
  summarise(geometry = st_combine(geometry)) %>%
  ungroup() %>%
  # mutate(geometry = st_cast(geometry, "POLYGON")) %>% # Not sure if we should do this. But it's easier to work with (pan/zoom to/highlight) individual polygons than a multipolygon in leaflet. Especially discontiguous ones like AVEC
  inner_join(certificates_csv, by=join_by(certificate_number)) %>%
  select(-geometry,everything()) # Move geometry to end

#sf_use_s2(FALSE)
#tmap_mode("view")
#tm_shape(merged) +
#  tm_polygons(fill_alpha=0.5)

st_write(merged_raw, glue("service-areas-raw-all.geojson"))
st_write(merged_raw %>% filter(utility_type == "Electric"), glue("service-areas-raw-electric.geojson"))
st_write(merged_raw %>% filter(utility_type != "Electric"), glue("service-areas-raw-nonelectric.geojson"))



#### Begin manual patching
# Below this line we are modifying/deleting service areas (for good reason), however, 
# I think it's valuable to provide a raw, imperfect merged layer as well, which is what the above exported geojson are.


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
  # https://github.com/acep-uaf/utility-service-areas/issues/9#issuecomment-3054393380
  121,
  # https://github.com/acep-uaf/utility-service-areas/issues/19
  523
)

certificates_csv.electric_filtered <- certificates_csv %>%
  mutate(entity_type = ifelse(
    certificate_number %in% operator_ids,
    "operator",
    "utility")) %>%
  filter(!(certificate_number %in% inactive_ids),
         `certificate_status` == "Active",
         `utility_type` == "Electric")

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


for (i in 1:nrow(certificates_csv.electric_filtered)) {
  cur_certificate_number <- certificates_csv.electric_filtered[i, ]$certificate_number
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

certificates <- certificates_csv.electric_filtered %>%
  rowwise() %>%
  mutate(kml_desc_field = safe_read_kml_description(certificate_number)) %>%
  ungroup() %>%
  separate_wider_regex(
    kml_desc_field,
    patterns = c(
      "Granted to: ",
      kml_utility_name = "[-A-Za-z/().,&\\\\ ]+",
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
  mutate(kml_has_latest_certificate_update = ifelse( # It doesn't really matter whether an inactive certificate has an up-to-date KML
    certificate_status == "Active",
    kml_has_latest_certificate_update,
    NA
  )) %>%
  relocate(kml_most_recent_update_date, .after = kml_most_recent_update_included) %>%
  ungroup()

# BEGIN MANUAL PATCHING OF SERVICE AREAS

patch_effective_versions <- tribble(
  # We don't want to mess around and merge kmls together unless we're absolutely sure that we are correcting data and not introducing errors.
  # This table contains a certificate number and a last update date.
  # If the KML last update date does not match the date in the table, then the patches will be skipped for that certificate.
  # In this way, the patches will only be performed on a specific version of the KML that is known to have certain missing data.
  # To use this table, add a certificate being patched below and the current date of the last update in the KML (from kml_most_recent_update_date field). 
  # When RCA updates their KMLs, the patches will be skipped as the dates will not match anymore.
  ~cert, ~expected_kml_most_recent_update_date,
  169, "2002-03-26",
  8, "2013-01-25",
  635, "2001-07-05",
  412, "1988-11-28",
  365, "1990-04-25"
)

merge_patches <- tribble(
  # Define which certs are being manually merged/patched
  # Format: Cert1 += Cert2 -- Bigger cert # in first column, smaller acquired cert # in second column
  ~cert1, ~cert2,
  ## Start AVEC https://rca.alaska.gov/RCAWeb/ViewFile.aspx?id=6e731ed9-5898-4eaa-814c-8d1547ddac1a
  169, 61, # AVEC acquired service area of Teller (cert #81)
  169, 285, # AVEC acquired service area of City of Kotlik (cert #285) https://rca.alaska.gov/RCAWeb/ViewFile.aspx?id=598DFC13-86CF-47A0-A1A3-09245D482BA9
  169, 688, # AVEC acquired service area of City of Ekwok (cert #688) https://rca.alaska.gov/RCAWeb/ViewFile.aspx?id=8c915917-efe4-41ca-8d7d-1cb7534b19a9
  169, 407, # AVEC acquired service area of City of Kobuk (cert #407) https://rca.alaska.gov/RCAWeb/ViewFile.aspx?id=2f3dee1f-e202-434b-8f0a-714714ca3682
  169, 43, # AVEC acquired service area of Bethel Utilities Corporation (cert #43) https://rca.alaska.gov/RCAWeb/ViewFile.aspx?id=6eeb6425-ce1f-4c2f-b765-dfd3505e299f
  # Yakutat is missing a KML, so we can't merge it in
  169, 729, # AVEC acquired service area of Twin Hills (cert #729) https://rca.alaska.gov/RCAWeb/ViewFile.aspx?id=677d0f01-25b2-4c01-bf9b-bea7358300f9
  ## End AVEC
  8, 121 # CEA acquired service area of ML&P (cert #121) https://github.com/acep-uaf/utility-service-areas/issues/9#issuecomment-3054393380
)

plss_patches <- tribble(
  ~cert, ~corrected_plss_description,
  # PLSS description format: principal meridian, 3 digit township # and direction, 3 digit range # and direction, two digit section # 
  635, c("S009N067W05", "S009N067W06", "S010N067W31", "S010N067W32"), # Fixing error in Akiak service area description https://github.com/acep-uaf/utility-service-areas/issues/11
  412, c("S010N068W31", "S010N069W36"), # Fixing error in Akiachak service area description https://github.com/acep-uaf/utility-service-areas/issues/12
  365, c("S001N086W19", "S001N086W20", "S001N086W21", "S001N086W28", "S001N086W29", "S001N086W30") # Fixing error in Chefornak service area description https://github.com/acep-uaf/utility-service-areas/issues/16
) %>% unnest(corrected_plss_description) %>%
  group_by(cert) %>%
  summarise(
    query_string = paste0(
      "(MTRS = '", corrected_plss_description, "')",
      collapse = " OR "
    ),
    .groups = "drop"
  ) %>%
  mutate(query_url = glue("https://arcgis.dnr.alaska.gov/arcgis/rest/services/OpenData/ReferenceGrid_PLSSgridUnclipped/MapServer/1/query?where={URLencode(query_string)}",
                            "&outFields=*&returnGeometry=true&f=geojson"))

for(i in 1:nrow(plss_patches)) { # TODO: rewrite for loops using pwalk/map
  row <- plss_patches[i,]
  orig_kml_row <- certificates %>% filter(certificate_number == row$cert)
  patch_version_row <- patch_effective_versions %>% filter(cert == row$cert)
  if (nrow(orig_kml_row) > 0 && nrow(patch_version_row) > 0) {
    if (orig_kml_row$kml_most_recent_update_date == patch_version_row$expected_kml_most_recent_update_date) {
      out_path = glue("data/{row$cert}-servicearea-plss-fix.kml")
      file.remove(out_path)
      st_write(st_union(st_read(row$query_url)), out_path)
      message(glue("PLSS patch for certificate {row$cert} saved to {out_path}"))
    } else {
      warning(glue(
        "Certificate {row$cert} not patched, expected KML date {patch_version_row$expected_kml_most_recent_update_date} but got {orig_kml_row$kml_most_recent_update_date}"
      ))
    }
  }
  
}

patch_geometry <- function(cert_num, geom, kml_date) {
  patch_rows <- merge_patches %>% filter(cert1 == cert_num)
  patch_version_row <- patch_effective_versions %>% filter(cert == cert_num)
  
  if (nrow(patch_rows) > 0 && nrow(patch_version_row) > 0) {
    if (kml_date == patch_version_row$expected_kml_most_recent_update_date) {
      patched_geom <- geom
      for (cert2 in patch_rows$cert2) {
        # TODO: get rid of 'st_as_s2(): dropping Z and/or M coordinate' console spam
        patch_geom <- st_geometry(st_read(glue("data/{cert2}-servicearea.kml"), quiet = TRUE))
        patched_geom <- st_union(patched_geom, patch_geom)
        message(glue("Patch applied to certificate {cert_num}, merged with certificate {cert2}"))
      }
      return(patched_geom)
    } else {
      warning(glue(
        "Certificate {cert_num} not patched, expected KML date {patch_version_row$expected_kml_most_recent_update_date} but got {kml_date}"
      ))
      return(geom)
    }
  } else {
    return(geom)
  }
}

# TODO: warning, kmls for certs_missing_kml_files are missing 

file_list <- list.files("data", pattern = "-servicearea(-plss-fix)?.kml$", full.names = TRUE)
files_tbl <- tibble(file = file_list) %>%
  mutate(
    cert = str_match(file, "^data/([\\d]+(\\.[\\d]+)?)-servicearea")[,2],
    is_patch = str_detect(file, "-plss-fix")
  ) %>%
  group_by(cert) %>%
  filter(is_patch | !any(is_patch)) %>% # If there is a patch, remove the original file from the file list
  ungroup() %>%
  pull(file)
sf_list <- map(files_tbl, ~st_read(.x, quiet = TRUE) %>%
                 mutate(file_path = .x))
merged_processed_electric_service_areas <- bind_rows(sf_list) %>%
  mutate(certificate_number = as.numeric(str_extract(file_path, regex("[\\d]+(\\.[\\d]+)?(?=-servicearea)")))) %>% # Regex needs to match CPCN "18.1"
  select(c(certificate_number, geometry)) %>%
  rowwise() %>%
  mutate(geometry = st_make_valid(geometry)) %>%
  ungroup() %>%
  group_by(certificate_number) %>%
  summarise(geometry = st_combine(geometry)) %>% # TODO: not sure if we want st_union or st_combine here. This line is here because of certs 725 and 726 which are multiple polygon features instead of one multipolygon
  ungroup() %>%
  # mutate(geometry = st_cast(geometry, "POLYGON")) %>% # Not sure if we should do this. But it's easier to work with (pan/zoom to/highlight) individual polygons than a multipolygon in leaflet. Especially discontiguous ones like AVEC
  inner_join(certificates, by=join_by(certificate_number)) %>%
  select(-geometry,everything()) %>% # Move geometry to end
  rowwise() %>%
  mutate(geometry = patch_geometry(certificate_number, geometry, kml_most_recent_update_date)) %>%
  ungroup()

# END MANUAL PATCHING

# Now we have a merged layer with active electric service areas, cleaned and processed according to our needs (e.g. excluding wholesale and IPP)
# and with manual patches to fix some data quality issues
output_path = "service-areas.geojson"
if (file.exists(output_path)) {
  file.remove(output_path)
}
st_write(merged_processed_electric_service_areas %>% 
           filter(entity_type == "utility"), output_path)

