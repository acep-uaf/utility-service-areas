submit_with_field_and_get_rca_response <- function(s, field_name, field_value) {
  form <- html_form(s)[[1]]
  # or
  # field_values <- lapply(form$fields, function(x) x$value) %>%
  # set_names(map_chr(form$fields, "name"))
  # Because __EVENTTARGET doesn't get detected as a field by rvest we can't use html_form_set/html_form_submit, instead we have to use httr
  field_values <- map(form$fields, "value") %>%
    set_names(map_chr(form$fields, "name"))
  field_values[[field_name]] <- field_value
  session_id <- (cookies(s) %>%
    filter(name == "ASP.NET_SessionId"))[1, ]$value
  response <- POST(
    url = form$action,
    body = field_values,
    encode = "form",
    set_cookies(`ASP.NET_SessionId` = session_id)
  ) %>%
    read_html()
  return(response)
}


fetch_certificates_list <- function() {
  s <- session("https://rca.alaska.gov/RCAWeb/home.aspx")
  # This query will return a list of all electric utilities certificated by RCA
  certificates_list_url <- "https://rca.alaska.gov/RCAWeb/RCALibrary/SearchResults.aspx?t=cert&p=typesearch&cert=&entity=&utiltype=fb3aa508-d4ce-40d9-8e4a-602912321bce"
  # if (!file.exists("rca_electric_certificates.csv")) {
  start.time <- Sys.time()
  message("Downloading list of electric certificates from RCA site...")
  s <- s %>% session_jump_to(certificates_list_url)
  certificate_count <- s %>%
    html_element(".count") %>%
    html_text2() %>%
    str_extract(regex("\\d+")) %>%
    as.numeric()
  if (certificate_count > 200) {
    stop("There are more than 200 electric certificates listed on the RCA site. Downloading more than 200 (i.e. paginating multiple pages of 200) is unimplemented")
  }
  message(glue("{certificate_count} certificates listed on RCA site, formatting and removing duplicates..."))
  response_table_element <- submit_with_field_and_get_rca_response(s, "PortalPageControl1:_ctl6:searchResultCert:certGridHeader:ddlNumberPerPage", "200") %>%
    html_element("table.RCAGrid")
  table <- response_table_element %>%
    html_table()
  links <- response_table_element %>%
    html_nodes(xpath = "//td/a")
  text_and_links <- tibble(
    text = links %>% html_text2(),
    href = links %>% html_attr("href")
  ) %>%
    distinct(text, .keep_all = TRUE) %>%
    filter(!(str_detect(href, regex("\\?id=$"))))
  colnames(table) <- table[2, ] %>%
    tolower() %>%
    gsub(" ", "_", .)
  out_certificate_csv <- table %>%
    slice(-(1:2), -nrow(table)) %>%
    distinct(certificate_number, .keep_all = TRUE) %>%
    left_join(text_and_links, by = join_by(certificate_number == text)) %>%
    rename(cpcn_url = href) %>%
    left_join(text_and_links, by = join_by(entity == text)) %>%
    rename(entity_url = href)
  out_certificate_csv$certificate_number <- as.numeric(out_certificate_csv$certificate_number)

  out_certificate_csv[is.na(out_certificate_csv)] <- ""
  message(glue("Saving list of {nrow(out_certificate_csv)} unique electric certificates to CSV..."))
  file_name <- glue("rca_electric_certificates_{Sys.Date()}.csv")
  write_csv(out_certificate_csv, file_name)
  end.time <- Sys.time()
  message(glue("Took {end.time - start.time} seconds to gather list of certificates from RCA site."))
  return(normalizePath(file_name))
  # }
}

init_data_directory <- function() {
  dir.create("data", showWarnings = FALSE)
  dir.create("data/missing-placeholder", showWarnings = FALSE)
  # unlink("data/missing-placeholder/*") # Probably don't want to do this on every run, because it makes it take forever to run the pipeline. But in general the pipeline should check that missing files are still missing and not rely on the presence of placeholder files.
  return(normalizePath("data"))
}

download_certificate_kml <- function(certificate_row) { # TODO: combine with pdf func and maybe chronology func
  cert_url <- certificate_row$cpcn_url
  cert_number <- certificate_row$certificate_number
  message(glue("Downloading KML for certificate {cert_number}..."))
  kml_out_path <- file.path("data", glue("{cert_number}-servicearea.kml"))
  if (file.exists(kml_out_path)) {
    message(glue("Skipped, KML for certificate {cert_number} was already downloaded"))
  } else {
    s <- session(cert_url)
    page_html <- s %>%
      read_html()
    kml_url <- page_html %>%
      html_element("a[href*='ViewFile.aspx'][id$='KMLDoc']") %>%
      html_attr("href")

    if (!is.na(kml_url)) {
      download.file(kml_url, destfile = kml_out_path)
      message(glue("KML for certificate {cert_number} successfully downloaded"))
    } else {
      warning(glue("Skipping download: KML URL for certificate {cert_number} not found."))
      placeholder_path <- file.path("data/missing-placeholder", glue("{cert_number}-kml-MISSING.txt"))
      file.create(placeholder_path)
      # message(glue("Created placeholder file at {placeholder_path} for missing KML of certificate {cert_number}."))
      # writeLines("MISSING", placeholder_path)
      return(normalizePath(placeholder_path))
    }
  }
  return(normalizePath(kml_out_path))
}


download_certificate_pdf <- function(certificate_row) {
  cert_url <- certificate_row$cpcn_url
  cert_number <- certificate_row$certificate_number
  cert_out_path <- file.path("data", glue("{cert_number}-certificate.pdf"))
  if (file.exists(cert_out_path)) {
    message(glue("Skipped, certificate {cert_number} was already downloaded"))
  } else {
    s <- session(cert_url)
    page_html <- s %>%
      read_html()
    cert_pdf_url <- page_html %>%
      html_element("a[href*='ViewFile.aspx'][id$='CertDoc']") %>%
      html_attr("href")

    if (!is.na(cert_pdf_url)) {
      download.file(cert_pdf_url, destfile = cert_out_path)
      message(glue("PDF of certificate {cert_number} successfully downloaded"))
    } else {
      warning(glue("Skipping download: PDF URL for certificate {cert_number} not found."))
      placeholder_path <- file.path("data/missing-placeholder", glue("{cert_number}-certificate-pdf-MISSING.txt"))
      file.create(placeholder_path)
      return(normalizePath(placeholder_path))
    }
  }
  return(normalizePath(cert_out_path))
}

download_certificate_chronology <- function(certificate_row) {
  cert_url <- certificate_row$cpcn_url
  cert_number <- certificate_row$certificate_number
  chronology_out_path <- file.path("data", glue("{cert_number}-certificate-chronology.html"))
  if (file.exists(chronology_out_path)) {
    message(glue("Skipped, chronology for certificate {cert_number} was already downloaded"))
  } else {
    s <- session(cert_url)
    start.time <- Sys.time()
    response <- submit_with_field_and_get_rca_response(s, "__EVENTTARGET", "PortalPageControl1$_ctl6$PortalPageControl1$TabLink1")
    if (!is.na(html_element(response, "a.activePortalTabLink[id$='TabLink1']"))) {
      xml2::write_xml(response, chronology_out_path)
      end.time <- Sys.time()
      message(glue("Took {end.time - start.time} seconds to get chronology for certificate {cert_number}."))
    } else {
      warning(glue("Skipping download: Error loading chronology page for certificate {cert_number}."))
      placeholder_path <- file.path("data/missing-placeholder", glue("{cert_number}-chronology-MISSING.txt"))
      file.create(placeholder_path)
      return(normalizePath(placeholder_path))
    }
  }
  return(normalizePath(chronology_out_path))
}

st_write_or_overwrite <- function(sf_object, file_path) {
  if (file.exists(file_path)) {
    file.remove(file_path)
  }
  st_write(sf_object, file_path)
}

generate_and_export_raw_geojson <- function(kml_file_paths, certificates_csv, out_file) {
  # Filter out missing placeholder files created earlier
  kml_file_paths <- kml_file_paths %>%
    str_subset(pattern = "\\.kml$")
  sf_list <- map(kml_file_paths, ~ st_read(.x, quiet = TRUE))

  merged <- bind_rows(sf_list) %>%
    mutate(certificate_number = as.numeric(str_extract(Name, regex("(?!Certificate No. )[\\d]+(\\.[\\d]+)?")))) %>% # Regex needs to match CPCN "18.1"
    select(-c(Name, Description)) %>%
    mutate(geometry = st_make_valid(geometry)) %>%
    group_by(certificate_number) %>%
    summarise(geometry = st_combine(geometry)) %>%
    ungroup() %>%
    inner_join(certificates_csv, by = join_by(certificate_number)) %>%
    select(-geometry, everything()) # Move geometry to end
  # TODO: possibly combine with patch logic? To avoid having two functions that create geojson files with a lot of duplicate logic
  st_write_or_overwrite(merged, out_file)

  return(normalizePath(out_file))
}

filter_certificates_csv <- function(certificates_csv, operator_ids, inactive_ids) {
  # This code adds a new field to certificates_csv - "entity_type" (utility or operator)
  # An operator produces electricity and sells all its electric output to a utility or utilities.
  # A utility distributes/delivers electricity to the customers who live in its service area. Most utilities in Alaska generate some or all of their own electricity (in other words, not all utilities purchase electricity from operators).
  # RCA issues certificates to utilities, and also issues certificates to operators, who sell power to utilities, but don't actually service any households or customers.
  # Operators do not have "service areas" and the ones assigned by RCA do not make very much sense.
  # For example, it might be a circle around a wind farm or a random spot in a lake.
  # We need to add this field so we can easily distinguish between these two types of entities, enabling us to exclude the psuedo-service areas of operators.
  # TODO: the operator/utility distinction is pretty crude, should use IPP/wholesale terminology instead, see github issue
  # https://github.com/acep-uaf/utility-service-areas/issues/18



  certificates_csv_filtered <- certificates_csv %>%
    mutate(entity_type = ifelse(
      certificate_number %in% operator_ids,
      "operator",
      "utility"
    )) %>%
    filter(
      # Delete rows representing certificates that are inactive but mistakenly are still assigned active status by RCA
      !(certificate_number %in% inactive_ids),
      certificate_status == "Active",
      # utility_type == "Electric",
      entity_type == "utility"
    )

  # Return the cleaned certificates dataframe, exluding "operator" entities and inactive utilities
  return(certificates_csv_filtered)
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

safe_read_kml_description <- purrr::possibly(read_kml_description, otherwise = NA_character_)

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

process_chronology <- function(certificate_number) {
  read_chronology_table(certificate_number) %>%
    mutate(`Order Date` = mdy(`Order Date`)) %>%
    arrange(`Order Date`) %>%
    mutate(Certificate = certificate_number) %>%
    mutate(Order = as.character(Order)) %>%
    select(c(Certificate, `Docket Number`, Order, `Order Date`), everything())
}

convert_two_digit_years <- function(x, year = 1963) {
  # The oldest year appearing in RCA chronologies is 1964
  # https://stackoverflow.com/questions/12323693/convert-two-digit-years-to-four-digit-years-with-correct-century/12957909#12957909
  m <- year(x) %% 100
  year(x) <- ifelse(m > year %% 100, 1900 + m, 2000 + m)
  return(x)
}

kml_has_newest_service_area_updates <- function(cert_num, kml_update_date, chronology_df) {
  chronology_entries <- chronology_df %>%
    filter(Certificate == cert_num)
  if ((chronology_entries %>% tail(n = 1))$`Order Date` < kml_update_date) {
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

build_certificates_df <- function(input_certs_df, certificates_chronology) {
  
  get_latest_chronology_for_cert <- function(cert_number) {
    x <- certificates_chronology %>%
      filter(Certificate == cert_number) %>%
      tail(n = 1)
  }
  
  get_formed_year_for_cert <- function(cert_number) {
    x <- certificates_chronology %>%
      filter(Certificate == cert_number) %>%
      head(n = 1)
    return(year(x$`Order Date`))
  }
  
  build_order_text <- function(cert_number) {
    docket_number <- get_latest_chronology_for_cert(cert_number)$`Docket Number`
    order_number <- get_latest_chronology_for_cert(cert_number)$Order
    if (!is.na(docket_number) && !is.na(order_number) && docket_number != "" && order_number != "") {
      return(glue("{docket_number}({order_number})"))
    } else if (!is.na(docket_number) && docket_number != "") {
      return(docket_number)
    } else {
      return(NA_character_)
    }
  }
  
  certificates <- input_certs_df %>%
    rowwise() %>%
    mutate(kml_desc_field = safe_read_kml_description(certificate_number)) %>% # TODO: custom patched kmls should have description  matching RCAs so that we can extract dates and stuff automatically rather manually setting in 20 places
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
    mutate(certificate_last_update_date = (certificates_chronology %>%
      filter(Certificate == certificate_number) %>%
      tail(n = 1))$`Order Date`) %>%
    # format('%m/%d/%y')) %>% # Temporarily change to MM/DD/YY so we don't get issues like xx/xx/64 (in KML descriptions) being converted to 2064-xx-xx. This will break in a few decades though. Fixed by convert_two_digit_years function
    mutate(certificate_granted_year = get_formed_year_for_cert(certificate_number)) %>%    
    mutate(certificate_last_update_type = get_latest_chronology_for_cert(certificate_number)$Type) %>%
    mutate(certificate_last_update_description = get_latest_chronology_for_cert(certificate_number)$Comment) %>%
    mutate(certificate_last_update_order = build_order_text(certificate_number)) %>%
    mutate(kml_most_recent_update_date = str_extract(kml_most_recent_update_included, "[\\d]{1,2}\\/[\\d]{2}\\/([\\d]{4}|[\\d]{2})") %>%
      mdy() %>%
      convert_two_digit_years()) %>%
    # format('%m/%d/%y')) %>%
    mutate(kml_has_latest_certificate_update = ifelse(
      is.na(kml_most_recent_update_date),
      NA,
      kml_has_newest_service_area_updates(certificate_number, kml_most_recent_update_date, certificates_chronology)
    )) %>%
    # This is now moot since inactive certificates are already filtered out earlier in the pipeline
    # mutate(kml_has_latest_certificate_update = ifelse( # It doesn't really matter whether an inactive certificate has an up-to-date KML
    #  certificate_status == "Active",
    #  kml_has_latest_certificate_update,
    #  NA
    # )) %>%
    relocate(kml_most_recent_update_date, .after = kml_most_recent_update_included) %>%
    ungroup()
}

format_plss_patches <- function(plss_patches) {
  plss_patches %>%
    unnest(corrected_plss_description) %>%
    group_by(cert) %>%
    summarise(
      query_string = paste0(
        "(MTRS = '", corrected_plss_description, "')",
        collapse = " OR "
      ),
      .groups = "drop"
    ) %>%
    mutate(query_url = glue(
      "https://arcgis.dnr.alaska.gov/arcgis/rest/services/OpenData/ReferenceGrid_PLSSgridUnclipped/MapServer/1/query?where={URLencode(query_string)}",
      "&returnGeometry=true&f=geojson"
    ))
}

save_plss_patches <- function(patch, certificates, patch_effective_versions) {
  orig_kml_row <- certificates %>% filter(certificate_number == patch$cert)
  patch_version_row <- patch_effective_versions %>% filter(cert == patch$cert)
  if (nrow(orig_kml_row) > 0 && nrow(patch_version_row) > 0) {
    if (orig_kml_row$kml_most_recent_update_date == patch_version_row$expected_kml_most_recent_update_date || is.na(patch_version_row$expected_kml_most_recent_update_date)) {
      out_path <- glue("data/{patch$cert}-servicearea-plss-fix.kml")
      st_write_or_overwrite(st_union(st_read(patch$query_url)), out_path)
      message(glue("PLSS patch for certificate {patch$cert} saved to {out_path}"))
      return(normalizePath(out_path))
    } else {
      message(glue(
        "Certificate {patch$cert} not patched, expected KML date {patch_version_row$expected_kml_most_recent_update_date} but got {orig_kml_row$kml_most_recent_update_date}"
      ))
      placeholder_path <- file.path("data/missing-placeholder", glue("{patch$cert}-patch-failed.txt"))
      file.create(placeholder_path)
      return(normalizePath(placeholder_path))
    }
  }
}

generate_and_export_geojson <- function(kml_file_paths, certificates, out_file, merge_patches, patch_effective_versions) {
  get_merge_geom <- function(cert_num, geom, kml_date) {
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

  # Filter out missing placeholder files created earlier
  kml_file_paths <- kml_file_paths %>%
    str_subset(pattern = "\\.kml$") # Only keep KML files, not placeholder files

  files_tbl <- tibble(file = kml_file_paths) %>%
    mutate(
      cert = str_match(file, "data/([\\d]+(\\.[\\d]+)?)-servicearea")[, 2],
      is_patch = str_detect(file, "-plss-fix")
    ) %>%
    group_by(cert) %>%
    filter(is_patch | !any(is_patch)) %>% # If there is a patch, remove the original file from the file list
    ungroup() %>%
    pull(file)

  sf_list <- map(files_tbl, ~ st_read(.x, quiet = TRUE) %>%
    mutate(file_path = .x))

  merged_patched <- bind_rows(sf_list) %>%
    mutate(certificate_number = as.numeric(str_extract(file_path, regex("[\\d]+(\\.[\\d]+)?(?=-servicearea)")))) %>% # Regex needs to match CPCN "18.1"
    # TODO: add name and description fields to patch KMLs, so that they match RCA's KMLs. This will simplify pipeline logic - no need for seperate regex above - and enable RCA to use the patches as well.
    select(c(certificate_number, geometry)) %>%
    rowwise() %>%
    mutate(geometry = st_make_valid(geometry)) %>%
    ungroup() %>%
    group_by(certificate_number) %>%
    summarise(geometry = st_combine(geometry)) %>% # TODO: not sure if we want st_union or st_combine here. This line is here because of certs 725 and 726 which are multiple polygon features instead of one multipolygon
    ungroup() %>%
    inner_join(certificates, by = join_by(certificate_number)) %>%
    select(-geometry, everything()) %>% # Move geometry to end
    rowwise() %>%
    mutate(geometry = get_merge_geom(certificate_number, geometry, kml_most_recent_update_date)) %>%
    ungroup()
  
  # Reformat field names
  
#  [1] "certificate_number"                  "certificate_type"                    "entity"                             
#  [4] "certificate_name"                    "utility_type"                        "certificate_status"                 
#  [7] "cpcn_url"                            "entity_url"                          "entity_type"                        
 # [10] "alt_name"                            "kml_most_recent_update_included"     "kml_most_recent_update_date"        
#  [13] "certificate_last_update_date"        "certificate_last_update_type"        "certificate_last_update_description"
#  [16] "kml_has_latest_certificate_update" 
  
  # set_sync_status <- function
  
  set_sync_string <- function(is_current) {
    if (is.na(is_current)) {
      return("unknown")
    } else if (is_current) {
      return("up_to_date")
    } else {
      return("outdated")
    }
  }
  
  merged_patched <- merged_patched %>%
    rename(geometry_is_current = kml_has_latest_certificate_update) %>%
    rename(certificate_url = cpcn_url) %>%
    # service_area_geometry_effective_date?
    rename(geometry_last_update = kml_most_recent_update_date) %>% # Have to manually set this for patches
    select(certificate_number,
           entity,
           certificate_name,
           # utility_type, All electric
           # certificate_status, All active
           certificate_url,
           certificate_granted_year,
           certificate_last_update_date,
           certificate_last_update_order,
           certificate_last_update_type,
           geometry_last_update,
           geometry_is_current
           # certificate_last_update_description, Too messy to include
           ) %>%
    mutate(certificate_last_update_type = ifelse(certificate_last_update_type == "type not set", 
                                                 NA_character_,
                                                 certificate_last_update_type)) %>%
    rowwise() %>%
    mutate(geometry_cert_sync_status = set_sync_string(geometry_is_current))
    #rowwise() %>%
    #mutate(
    #  certificate_last_update_type = as.logical(certificate_last_update_type),
    #  geometry_certificate_sync_status = case_when(
        # Todo - add up_to_date_manually_patched
    #    is.na(certificate_last_update_type) ~ "unknown",
    ##    certificate_last_update_type == TRUE ~ "up_to_date",
      #  !certificate_last_update_type == FALSE ~ "outdated"
    #  )
    #)
  
  # 
    
  
  
  # Fields
  # certificate_number
  # certificate_type
  # utility_type
  # entity - not really useful
  # certificate_name
  # certificate_status
  # latest_chronology_entry
  # latest_order
  # service_area_as_of
  # cpcn_url ---> change to certificate_url
  # entity_url  -- drop
  # 

  st_write_or_overwrite(merged_patched, out_file)

  return(normalizePath(out_file))
}
