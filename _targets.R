# Created by use_targets().
# Follow the comments below to fill in this target script.
# Then follow the manual to check and run the pipeline:
#   https://books.ropensci.org/targets/walkthrough.html#inspect-the-pipeline

# Load packages required to define the pipeline:
library(targets)
# library(tarchetypes) # Load other packages as needed.

# Set target options:
tar_option_set(
  packages = c("tibble", "rvest", "tidyverse", "glue", "httr", "sf"), # Packages that your targets need for their tasks.
  # format = "qs", # Optionally set the default storage format. qs is fast.
)

# Run the R scripts in the R/ folder with your custom functions:
tar_source()
# tar_source("other_functions.R") # Source other scripts as needed.

# Replace the target list below with your own:
list(
  tar_target(
    name = certificates_csv,
    command = fetch_certificates_list(),
    format = "file"
  ),
  tar_target(
    name = certificates_csv_df,
    command = read_csv(certificates_csv),
  ),
  tar_target(
    name = data_directory,
    command = init_data_directory(),
    format = "file"
  ),
  tar_target(
    name = certificate_kmls,
    command = download_certificate_kml(certificates_csv_df),
    pattern = map(certificates_csv_df),
    format = "file"
  ),
  tar_target(
    name = certificate_pdfs,
    command = download_certificate_pdf(certificates_csv_df),
    pattern = map(certificates_csv_df),
    format = "file"
  ),
  tar_target(
    name = certificate_chronologies,
    command = download_certificate_chronology(certificates_csv_df),
    pattern = map(certificates_csv_df),
    format = "file"
  ),
  # Just merge all electric service areas, active and inactive. No manual filtering/patching yet.
  # It's pretty common for inactive service areas, especially old/obscure ones, to not have kmls.
  # So this "raw" layer will still have less rows than the total # of electric utilities.
  tar_target(
    name = electric_service_areas_raw,
    command = generate_and_export_raw_geojson(certificate_kmls, certificates_csv_df, "service-areas-raw.geojson"),
    format = "file"
  ),
  # Below this line we are modifying/deleting service areas (for good reason), however,
  # I think it's valuable to provide a raw, imperfect merged layer as well, which is what the above exported geojson is.
  tar_target(
    name = operator_ids,
    command = c(
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
  ),
  tar_target(
    name = inactive_ids,
    command = c(
      # https://github.com/acep-uaf/utility-service-areas/issues/23
      59,
      # https://github.com/acep-uaf/utility-service-areas/issues/15
      71,
      # https://github.com/acep-uaf/utility-service-areas/issues/49
      91,
      # https://github.com/acep-uaf/utility-service-areas/issues/9#issuecomment-3054393380
      121,
      # https://github.com/acep-uaf/utility-service-areas/issues/19
      523
    )
  ),
  tar_target(
    name = certificates_csv_df_cleaned,
    command = filter_certificates_csv(certificates_csv_df, operator_ids, inactive_ids)
  ),
  tar_target(
    name = certificates_chronology,
    command = process_chronology(certificates_csv_df_cleaned$certificate_number),
    pattern = map(certificates_csv_df_cleaned),
  ),
  tar_target(
    name = certificates_with_kml_and_chronology_metadata,
    command = build_certificates_df(certificates_csv_df_cleaned, certificates_chronology),
  ),
  tar_target(
    # We don't want to mess around and merge kmls together unless we're absolutely sure that we are correcting data and not introducing errors.
    # This table contains a certificate number and a last update date.
    # If the KML last update date does not match the date in the table, then the patches will be skipped for that certificate.
    # In this way, the patches will only be performed on a specific version of the KML that is known to have certain missing data.
    # To use this table, add a certificate being patched below and the current date of the last update in the KML (from kml_most_recent_update_date field).
    # When RCA updates their KMLs, the patches will be skipped as the dates will not match anymore.
    name = patch_effective_versions,
    command = tribble(
      ~cert, ~expected_kml_most_recent_update_date,
      169, "2002-03-26",
      8, "2013-01-25",
      635, "2001-07-05",
      412, "1988-11-28",
      365, "1990-04-25",
      395, "2002-11-12",
      # KMLs missing from RCA's site
      741, NA,
      767, NA
    )
  ),
  tar_target(
    # Define which certs are being manually merged/patched
    name = merge_patches,
    command = tribble(
      # Format: Cert1 += Cert2 -- Bigger cert # in first column, smaller acquired cert # in second column
      ~cert1, ~cert2,
      ## Start AVEC https://rca.alaska.gov/RCAWeb/ViewFile.aspx?id=6e731ed9-5898-4eaa-814c-8d1547ddac1a
      169, 61, # AVEC acquired service area of Teller (cert #61)
      169, 285, # AVEC acquired service area of City of Kotlik (cert #285) https://rca.alaska.gov/RCAWeb/ViewFile.aspx?id=598DFC13-86CF-47A0-A1A3-09245D482BA9
      169, 688, # AVEC acquired service area of City of Ekwok (cert #688) https://rca.alaska.gov/RCAWeb/ViewFile.aspx?id=8c915917-efe4-41ca-8d7d-1cb7534b19a9
      169, 407, # AVEC acquired service area of City of Kobuk (cert #407) https://rca.alaska.gov/RCAWeb/ViewFile.aspx?id=2f3dee1f-e202-434b-8f0a-714714ca3682
      169, 43, # AVEC acquired service area of Bethel Utilities Corporation (cert #43) https://rca.alaska.gov/RCAWeb/ViewFile.aspx?id=6eeb6425-ce1f-4c2f-b765-dfd3505e299f
      # Yakutat is missing a KML, so we can't merge it in
      169, 729, # AVEC acquired service area of Twin Hills (cert #729) https://rca.alaska.gov/RCAWeb/ViewFile.aspx?id=677d0f01-25b2-4c01-bf9b-bea7358300f9
      ## End AVEC
      8, 121 # CEA acquired service area of ML&P (cert #121) https://github.com/acep-uaf/utility-service-areas/issues/9#issuecomment-3054393380
    )
  ),
  tar_target(
    name = plss_patches,
    command = tribble(
      ~cert, ~corrected_plss_description,
      # PLSS description format: principal meridian, 3 digit township # and direction, 3 digit range # and direction, two digit section # 
      635, c("S009N067W05", "S009N067W06", "S010N067W31", "S010N067W32"), # Fixing error in Akiak service area description https://github.com/acep-uaf/utility-service-areas/issues/11
      412, c("S010N068W31", "S010N069W36"), # Fixing error in Akiachak service area description https://github.com/acep-uaf/utility-service-areas/issues/12
      365, c("S001N086W19", "S001N086W20", "S001N086W21", "S001N086W28", "S001N086W29", "S001N086W30"), # Fixing error in Chefornak service area description https://github.com/acep-uaf/utility-service-areas/issues/16,
      395, c("S002S079W28", "S002S079W29", "S002S079W32", "S002S079W33"), # Fixing error in Puvurnaq Power Company service area description https://github.com/acep-uaf/utility-service-areas/issues/47
      # Creating missing KMLs https://github.com/acep-uaf/utility-service-areas/issues/8
      741, c("K018S010W28", "K018S010W29", "K018S010W30", "K018S010W31", "K018S010W32",
            "K018S011W09", "K018S011W16", "K018S011W21", "K018S011W22", "K018S011W23",
            "K018S011W26", "K018S011W27", "K018S011W28", "K018S011W34", "K018S011W35", "K018S011W36",
            "K019S011W01", "K019S011W02", "K019S011W03"), # Unalakleet Valley Electric
      767, c("F017N009E21", "F017N009E22", "F017N009E27", "F017N009E28", "F017N009E33", "F017N009E34") # Birch Creek Tribal Council
    ) %>% format_plss_patches()
  ),
  tar_target(
    name = plss_patches_kmls,
    command = save_plss_patches(plss_patches, certificates_with_kml_and_chronology_metadata, patch_effective_versions),
    pattern = map(plss_patches),
    format = "file"
  ),
  tar_target(
    name = electric_service_areas_cleaned_patched,
    command = generate_and_export_geojson(append(certificate_kmls, plss_patches_kmls), certificates_with_kml_and_chronology_metadata, "service-areas-v2-metadata.geojson", merge_patches, patch_effective_versions),
    format = "file"
  )#,
  #### for dev
  #tar_target(
  #  name = output_no_geom,
  #  command = electric_service_areas_cleaned_patched %>% st_set_geometry(NULL)
  #)
)
