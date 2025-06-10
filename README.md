# utility-service-areas -- DRAFT

This repository contains the source code for the data pipeline that processes KML files of authorized service areas for electric utilities in Alaska. Utilities in Alaska are regulated by the Regulatory Commission of Alaska, which provides the boundaries of these service areas to the public through their website. These boundaries are invaluable for reseachers who are studying energy rates in communities across Alaska, especially since the majority of Alaskans are served by microgrid systems which are not interconnected to a broader traditional power grid. 

The goal of this project is to create a single geospatial file in GeoJSON format which contains the service area boundaries of every active electric utility in Alaska. No such file currently exists, and creating one manually by downloading and combining hundreds of KML files from the RCA website would be time-consuming and tedious. The creation of this resource will support future Alaskan energy research, and make it easier to answer questions like:
1. Do electrical service areas overlap?
2. Which utility serves the largest area? The smallest?
3. What utility serves a specific location or community?
