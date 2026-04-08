# Copyright (c) Kartik Sharma. GitHub: kartiksharma4448
from __future__ import annotations

from universal_index.providers.bhuvan import fetch_bhuvan_soil_context
from universal_index.providers.copernicus import fetch_copernicus_climate_context
from universal_index.providers.imd import fetch_imd_climate_context
from universal_index.providers.open_meteo import fetch_open_meteo_climate_context
from universal_index.providers.soilgrids import fetch_soilgrids_soil_context
from universal_index.providers.agristack_proxy import build_agristack_proxy_context

__all__ = [
	"fetch_bhuvan_soil_context",
	"fetch_copernicus_climate_context",
	"fetch_imd_climate_context",
	"fetch_open_meteo_climate_context",
	"fetch_soilgrids_soil_context",
	"build_agristack_proxy_context",
]
